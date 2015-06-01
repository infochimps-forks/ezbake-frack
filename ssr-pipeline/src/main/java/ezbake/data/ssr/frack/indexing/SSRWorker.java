/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.data.ssr.frack.indexing;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.search.SsrServiceConstants;
import ezbake.services.search.ssrService;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SSRWorker extends Worker<SSRJSON> {
    private static Logger logger = LoggerFactory.getLogger(SSRWorker.class);
    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;
    private SSRBatchWriter batchWriter;

    public SSRWorker(Class<SSRJSON> type) {
        super(type);
    }

    @Override
    public void initialize(Properties props) {
        pool = new ThriftClientPool(props);
        EzProperties ezProps = new EzProperties(props, true);
        securityClient = new EzbakeSecurityClient(ezProps);
        batchWriter = new SSRBatchWriter(ezProps.getInteger("ssr.batch.max.queue.size", 50));
        Timer batchWriterTimer = new Timer();
        batchWriterTimer.scheduleAtFixedRate(batchWriter, 0, ezProps.getLong("ssr.batch.timeout", 15000));
    }

    @Override
    public void process(Visibility visibility, SSRJSON ssrjson) {
        batchWriter.addDocument(ssrjson.getSsr(), ssrjson.getJsonString());
    }

    class SSRBatchWriter extends TimerTask {
        private Map<SSR, String> pendingDocs;
        private Map<String, SSR> uriToSSR;
        private int maxQueueSize = 0;
        private final Object lock;

        @SuppressWarnings("unused")
        public SSRBatchWriter() {
            this(50);
        }

        public SSRBatchWriter(int maxQueueSize) {
            pendingDocs = Maps.newHashMap();
            uriToSSR = Maps.newHashMap();
            this.maxQueueSize = maxQueueSize;
            this.lock = new Object();
            logger.info("Creating SSR Batch Writer with max queue size of: {}", maxQueueSize);
        }

        @SuppressWarnings("unused")
        public void setMaxQueueSize(int size) {
            maxQueueSize = size;
        }

        public void addDocument(SSR ssr, String json) {
            synchronized(lock) {
                ssr.setTimeOfIngest(ezbake.data.common.TimeUtil.getCurrentThriftDateTime());
                pendingDocs.put(ssr, json);
                uriToSSR.put(ssr.getUri(), ssr);
                if(pendingDocs.size() > maxQueueSize) {
                    logger.debug("Hit max queue size - flushing queue");
                    flushQueue();
                }
            }
        }

        @SuppressWarnings("ThrowFromFinallyBlock")
        public void flushQueue() {
            boolean successful = false;
            List<IndexResponse> responses = Lists.newArrayList();
            ssrService.Client ssrClient = null;
            Exception thrownDuringPut = null;
            try {
                ssrClient = pool.getClient(SsrServiceConstants.SERVICE_NAME, ssrService.Client.class);
                Stopwatch stopwatch = Stopwatch.createStarted();
                responses = ssrClient.putWithDocs(pendingDocs, getSSRToken());
                successful = true;
                pool.returnToPool(ssrClient);
                logger.info("Indexed {} documents | {} ms", pendingDocs.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            } catch (TException e) {
                logger.error("Error indexing documents.", e);
                pool.returnBrokenToPool(ssrClient);
                thrownDuringPut = e;
            } finally {
                if (!successful) {
                    logger.error("Failed to index SSR's, sending to quarantine");
                    for (SSR key : pendingDocs.keySet()) {
                        AdditionalMetadata additionalMetadata = new AdditionalMetadata();
                        additionalMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(StackTraceUtil.getStackTrace(thrownDuringPut)));
                        additionalMetadata.putToEntries("URI", new MetadataEntry().setValue(key.getUri()));
                        try {
                            sendObjectToQuarantine(new SSRJSON(key, pendingDocs.get(key)),
                                                    key.getVisibility(),
                                                    "Error occurred while indexing documents. Check metadata for stack trace.",
                                                    additionalMetadata);
                        } catch (IOException e) {
                            logger.error("Could not send item to Quarantine, failing.", e);
                            throw new RuntimeException("Could not send item to Quarantine", e);
                        }
                    }
                } else if (!responses.isEmpty()) {
                    logger.debug("Checking responses for failures");
                    for (IndexResponse response : responses) {
                        if (!response.isSuccess()) {
                            SSR failed = uriToSSR.get(response.get_id());
                            AdditionalMetadata additionalMetadata = new AdditionalMetadata();
                            additionalMetadata.putToEntries("failure type", new MetadataEntry().setValue(response.get_type()));
                            additionalMetadata.putToEntries("URI", new MetadataEntry().setValue(failed.getUri()));
                            try {
                                sendObjectToQuarantine(new SSRJSON(failed, pendingDocs.get(failed)),
                                        failed.getVisibility(),
                                        "Error occurred while indexing documents. Check SSR service log.",
                                        additionalMetadata);
                            } catch (IOException e) {
                                logger.error("Could not send item to Quarantine, failing.", e);
                                throw new RuntimeException("Could not send item to Quarantine", e);
                            }
                        }
                    }
                }
                logger.debug("Clearing pending");
                pendingDocs.clear();
                uriToSSR.clear();
            }
        }

        private EzSecurityToken getSSRToken() {
            try {
                return securityClient.fetchAppToken();
            } catch (EzSecurityTokenException e) {
                logger.error("Token Exception", e);
            }
            throw new RuntimeException("Unable to refresh SSR token. Please see logs.");
        }

        @Override
        public void run() {
            synchronized (lock) {
                if(pendingDocs.size() > 0) {
                    logger.debug("Hit batch timeout - flushing queue");
                    flushQueue();
                }
            }
        }
    }
}
