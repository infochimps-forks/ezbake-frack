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

package ezbake.frack.common.workers;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Worker;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.warehaus.IngestStatus;
import ezbake.warehaus.IngestStatusEnum;
import ezbake.warehaus.PutRequest;
import ezbake.warehaus.PutUpdateEntry;
import ezbake.warehaus.UpdateEntry;
import ezbake.warehaus.WarehausService;
import ezbake.warehaus.WarehausServiceConstants;

/**
 * Batches warehaus entries from pipes and makes bulk updates to warehaus.
 *
 * @param <T> The class type of the object that is being stored.
 */
@SuppressWarnings("unused")
public final class WarehausBatchWorker<T extends Serializable> extends
                                            Worker<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory
            .getLogger(WarehausBatchWorker.class);

    public static final String BATCH_SIZE_KEY = "batch.writer.batch.size";
    private int maxQueueSize = 0;
    private ThriftClientPool pool;
    private String warehausSecurityId;
    private PutRequest putRequest;
    private EzbakeSecurityClient securityClient;
    @SuppressWarnings("unused")
    public static final String PIPELINE_SUFFIX = "_warehaus_batch_worker";
    private final IThriftConverter<T, UpdateEntry> fromObjectToEntry;
    private List<T> objects;
    private List<Visibility> visibilities;

    /**
     * Creates a WarehausBatchWorker object.
     * @param serializableObject the thrift definition that needs to be processed
     *                      by this worker.
     * @param fromObjectToEntry a thrift converter that implements conversion
     *        logic for given thrift definition to the warehaus
     *        {@link ezbake.warehaus.UpdateEntry} object.
     */
    @SuppressWarnings("unused")
    public WarehausBatchWorker(Class<T> serializableObject,
            IThriftConverter<T, UpdateEntry> fromObjectToEntry) {
        super(serializableObject);
        this.fromObjectToEntry = fromObjectToEntry;
    }

    /**
     * Initializes the warehaus batch worker with required variables.
     * @param props contains ezbake properties.
     */
    @Override
    public void initialize(Properties props) {
        EzProperties ezProps = new EzProperties(props, true);
        this.maxQueueSize = ezProps.getInteger(BATCH_SIZE_KEY, 50);
        this.pool = new ThriftClientPool(props);
        this.warehausSecurityId = pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME);
        this.putRequest = new PutRequest();
        this.securityClient = new EzbakeSecurityClient(props);
        objects = Lists.newArrayList();
        visibilities = Lists.newArrayList();
        Timer batchWriterTimer = new Timer();
        batchWriterTimer.scheduleAtFixedRate(new WarehausBatchTask(), 0,
                Long.valueOf(props.getProperty("warehouse.batch.timeout", String.valueOf(15000))));
        logger.info("Creating Batch Writer with max queue size of: {}",
                maxQueueSize);
    }

    /**
     * Batches together the thrift objects and makes bulk warehaus updates.
     * If the warehaus update fails, instead sends them to quarantine.
     * If successful, outputs the objects to the next pipe in the pipeline.
     */
    @Override
    public void process(Visibility visibility, T thriftObject) {
        synchronized (this) {
            IngestStatus status = null;
            try {
                UpdateEntry entry = fromObjectToEntry.convert(thriftObject);
                putRequest.addToEntries(new PutUpdateEntry(entry, visibility));
                objects.add(thriftObject);
                visibilities.add(visibility);
                if (putRequest.getEntriesSize() >= maxQueueSize) {
                    logger.debug("Hit max queue size - flushing queue");
                    status = flushQueue();
                    if (status != null && status.getStatus() != IngestStatusEnum.SUCCESS) {
                        throw new IOException("Failed to successfully ingest object to warehouse.");
                    }
                }
            } catch (TException ex) {
                quarantine(ex, null, thriftObject, visibility);
            } catch (IOException ioe) {
                quarantine(ioe, status);
            }
        }
    }

    /**
     * Flushes the entries accumulated this far and writes to the warehaus.
     *
     * @throws IOException
     *             throws if an error occurs during warehaus update
     */
    private IngestStatus flushQueue() throws IOException {
        WarehausService.Client warehaus = null;
        IngestStatus status;
        synchronized (this) {
            try {
                warehaus = pool.getClient(WarehausServiceConstants.SERVICE_NAME,
                                    WarehausService.Client.class);
                Stopwatch stopwatch = Stopwatch.createStarted();
                status = warehaus.put(putRequest, getWarehausToken());
                if (status.getStatus() != IngestStatusEnum.SUCCESS) {
                    return status;
                }
                logger.info("Indexed {} documents | {} ms",
                        putRequest.getEntriesSize(),
                        stopwatch.elapsed(TimeUnit.MILLISECONDS));
                logger.debug("Sending successfully ingested data to pipes");
                for (int i = 0 ; i < objects.size(); i++) {
                    T obj = objects.get(i);
                    Visibility v = visibilities.get(i);
                    outputToPipes(v, obj);
                }
                clearArtifacts();
                pool.returnToPool(warehaus);
            } catch (TException e) {
                logger.error("Error during batch indexing of documents.", e);
                pool.returnBrokenToPool(warehaus);
                throw new IOException(e);
            }
        }
        return status;
    }

    private EzSecurityToken getWarehausToken() {
        try {
            return securityClient.fetchAppToken(warehausSecurityId);
        } catch (EzSecurityTokenException e) {
            logger.error("Token Exception while retrieving security token for indexer", e);
        }
        throw new RuntimeException(
                "Unable to refresh warehaus token. Please see logs.");
    }

    private void quarantine(Exception e, IngestStatus status) {
        logger.debug("Sending failed ingests to quarantine");
        for (int i = 0 ; i < objects.size(); i++) {
            T obj = objects.get(i);
            try {
                UpdateEntry entry = fromObjectToEntry.convert(obj);
                // quarantine only if the object couldn't be ingested.
                if (!status.getFailedURIs().contains(entry.getUri())) {
                    continue;
                }
            } catch (TException ex) {
                logger.error("FATAL, cannot send object to Quarantine.", ex);
                throw new RuntimeException("Could not send object to Quarantine.", ex);
            }

            Visibility visibility = visibilities.get(i);
            quarantine(e, status, obj, visibility);
        }
        clearArtifacts();
    }

    private void quarantine(Exception e, IngestStatus status, T obj, Visibility visibility) {
        AdditionalMetadata metaData = new AdditionalMetadata();
        MetadataEntry stackTraceEntry = new MetadataEntry();
        stackTraceEntry.setValue(ExceptionUtils.getFullStackTrace(e));
        metaData.putToEntries("stackTrace", stackTraceEntry);
        MetadataEntry urnEntry = new MetadataEntry();
        try {
            UpdateEntry entry = fromObjectToEntry.convert(obj);
            urnEntry.setValue(entry.getUri());
            metaData.putToEntries("uri", urnEntry);
            if (status != null) {
                metaData.putToEntries("ingestStatus", new MetadataEntry().setValue(status.getStatus().toString()));
                if (StringUtils.isNotEmpty(status.getFailureReason())) {
                    metaData.putToEntries("failureReason", new MetadataEntry().setValue(status.getFailureReason()));
                }
            }

            sendObjectToQuarantine(obj, visibility,
                String.format("Could not batch ingest warehaus data"), metaData);
        } catch (TException | IOException ex) {
            logger.error("FATAL, cannot send object to Quarantine.", ex);
            throw new RuntimeException("Could not send object to Quarantine.", ex);
        }
    }

    private void clearArtifacts() {
        synchronized (this) {
            putRequest.getEntries().clear();
            objects.clear();
            visibilities.clear();
        }
    }

    public final class WarehausBatchTask extends TimerTask implements Serializable {

        public WarehausBatchTask() {
        }

        @Override
        public void run() {
            synchronized (this) {
                IngestStatus status = null;
                if (putRequest.getEntriesSize() > 0) {
                    try {
                        logger.debug("Hit batch timeout - flushing queue");
                        status = flushQueue();
                        if (status != null && status.getStatus() != IngestStatusEnum.SUCCESS) {
                            throw new IOException("Failed to successfully ingest object to warehouse.");
                        }
                    } catch (IOException ex) {
                        logger.error("Failed when executing WarehausBatchTask", ex);
                        quarantine(ex, status);
                    }
                }
            }
        }

    }
}