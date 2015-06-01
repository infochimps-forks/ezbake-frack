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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.thrift.AppNotRegisteredException;
import ezbake.thrift.ThriftClientPool;
import ezbake.warehaus.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

public class SSRJSONWorker extends Worker<SSRJSON> {
    private static Logger logger = LoggerFactory.getLogger(SSRJSONWorker.class);
    private ThriftClientPool pool;
    private EzbakeSecurityClient client;
    private String warehausSecurityId;

    public SSRJSONWorker(Class<SSRJSON> type) {
        super(type);
    }

    @Override
    public void process(Visibility visibility, SSRJSON object) {
        SSR ssr = object.getSsr();
        String url = ssr.getUri();
        WarehausService.Client warehaus = null;

        try {
            ViewId jsonViewId = new ViewId();
            jsonViewId.setUri(url);
            jsonViewId.setSpacename("SSR");
            jsonViewId.setView("JSON");
            jsonViewId.setSquashPrevious(true);

            warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);

            try {
                EzSecurityToken token = client.fetchAppToken(warehausSecurityId);

                ByteBuffer viewData = ByteBuffer.wrap(object.getJsonString().getBytes());
                IngestStatus status = warehaus.insertView(viewData, jsonViewId, ssr.getVisibility(), token);
                pool.returnToPool(warehaus);
                warehaus = null;
                if (status.getStatus() != IngestStatusEnum.SUCCESS) {
                    AdditionalMetadata metadata = new AdditionalMetadata();
                    metadata.putToEntries("URI", new MetadataEntry().setValue(object.getSsr().getUri()));
                    metadata.putToEntries("failureReason", new MetadataEntry().setValue(status.getFailureReason()));
                    try {
                        sendObjectToQuarantine(object, visibility, "Failed to insert view into Warehaus", metadata);
                    } catch (IOException e) {
                        logger.error("Could not send item to Quarantine.", e);
                        throw new RuntimeException("Quarantine is not available.", e);
                    }
                }
            } catch (AppNotRegisteredException e) {
                throw new RuntimeException("Could not interact with data warehaus because app is not registered", e);
            } catch (EzSecurityTokenException e) {
                handleFailure(object.getSsr().getUri(), e, object, visibility, "EzSecurityTokenException thrown during processing.");
                throw new RuntimeException("Could not properly retrieve security token", e);
            }
        } catch (TException e) {
            if (warehaus != null) {
                pool.returnBrokenToPool(warehaus);
            }
            handleFailure(object.getSsr().getUri(), e, object, visibility, "TException thrown during processing");
        }
    }

    private void handleFailure(String uri, Throwable e, SSRJSON failedObject, Visibility visibility, String error) {
        logger.error(String.format("Could not index uri %s in the warehaus. Check Warehaus and pipeline logs.", uri), e);
        AdditionalMetadata additionalMetadata = new AdditionalMetadata();
        additionalMetadata.putToEntries("stacktrace", new MetadataEntry().setValue(StackTraceUtil.getStackTrace(e)));
        additionalMetadata.putToEntries("URI", new MetadataEntry().setValue(uri));
        try {
            sendObjectToQuarantine(failedObject, visibility, error, additionalMetadata);
        } catch (IOException e1) {
            logger.error("Could not send item to Quarantine.", e1);
            throw new RuntimeException("Quarantine is not available.", e1);
        }
    }

    @Override
    public void initialize(Properties props) {
        pool = new ThriftClientPool(props);
        client = new EzbakeSecurityClient(props);
        warehausSecurityId = pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME);
    }
}
