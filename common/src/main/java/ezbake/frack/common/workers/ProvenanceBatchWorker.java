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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.thrift.ProvenanceRegistration;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.provenance.thrift.*;
import ezbake.thrift.ThriftClientPool;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Batches information about ingested documents for
 * registration with the Provenance service.
 *
 * @param <T> The class type of the document being registered with the Provenance service.
 */
public class ProvenanceBatchWorker<T extends Serializable> extends Worker<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceBatchWorker.class);

    public static final String PIPELINE_SUFFIX = "_provenance_worker";

    private final IThriftConverter<T, ProvenanceRegistration> itcProvenanceReg;

    public static final String BATCH_SIZE_KEY = "frack_common.provenance.batch_size";
    public static final String BATCH_TIMEOUT_KEY = "frack_common.provenance.batch_timeout_ms";
    public static final String OUTPUT_DUPLICATES_KEY = "frack_common.provenance.output_duplicates_to_pipes";

    public static final String NULL_URI_ERROR_MSG = "converter-produced-null-uri";
    @VisibleForTesting
    protected int maxQueueSize = 0;

    @VisibleForTesting
    protected ThriftClientPool pool;
    @VisibleForTesting
    protected EzbakeSecurityClient securityClient;

    // the Sets store data that will be sent to the Provenance service
    @VisibleForTesting
    protected Set<AddDocumentEntry> addDocumentEntrySet;
    @VisibleForTesting
    protected  Set<AgeOffMapping> ageOffMappingSet;
    @VisibleForTesting
    protected boolean outputDuplicates;
    private Long period;

    class UriToObjectItem {
        T object;
        Visibility visibility;

        UriToObjectItem(T object, Visibility visibility) {
            this.object = object;
            this.visibility = visibility;
        }
    }

    // the Map stores data to go to outputToPipes()
    @VisibleForTesting
    protected  Map<String, UriToObjectItem> uriToObjectMap;

    /**
     * Construct an instance of this class.
     *
     * @param type             The type of thrift object that will be passed into the
     *                         worker. Required.
     * @param itcProvenanceReg The Thrift converter that will convert thrift
     *                         object instances processed by this worker and turn them into
     *                         instances of {@link ezbake.frack.common.utils.thrift.ProvenanceRegistration}.
     */
    public ProvenanceBatchWorker(Class<T> type, IThriftConverter<T, ProvenanceRegistration> itcProvenanceReg) {

        super(type);
        this.itcProvenanceReg = itcProvenanceReg;
    }

    /**
     * Initializes the worker with configuration and actions needed prior to
     * processing.
     *
     * @param properties The application properties associated with the
     *                   environment.
     */
    public void initialize(Properties properties) {
        EzProperties ezProps = new EzProperties(properties, true);
        this.outputDuplicates = ezProps.getBoolean(OUTPUT_DUPLICATES_KEY, false);
        this.maxQueueSize = ezProps.getInteger(BATCH_SIZE_KEY, 50);
        this.pool = new ThriftClientPool(properties);
        this.securityClient = new EzbakeSecurityClient(properties);

        addDocumentEntrySet = Sets.newHashSet();
        ageOffMappingSet = Sets.newHashSet();
        uriToObjectMap = Maps.newHashMap();

        Timer batchWriterTimer = new Timer();
        period = Long.valueOf(properties.getProperty(BATCH_TIMEOUT_KEY, String.valueOf(15000)));
        batchWriterTimer.scheduleAtFixedRate(new ProvenanceBatchTask(), 0, period);
        logger.info("Creating ProvenanceBatchWorker with max queue size of: {}", maxQueueSize);
    }

    /**
     * Process the document to add it to the provenance service.
     *
     * @param visibility   The Accumulo visibility string representing the
     *                     classification level of the data contained in the incoming thrift object.
     * @param thriftObject The incoming Thrift object to be processed.
     */
    @Override
    public void process(Visibility visibility, T thriftObject) {
        synchronized (this) {
            try {
                ProvenanceRegistration provenanceReg = this.itcProvenanceReg.convert(thriftObject);
                if (provenanceReg.getAgeOffRules() != null) {
                    ageOffMappingSet.addAll(provenanceReg.getAgeOffRules());
                }

                String uri = provenanceReg.getUri();
                if (uri == null) {
                    quarantine(NULL_URI_ERROR_MSG, thriftObject, visibility);
                } else {
                    uriToObjectMap.put(uri, new UriToObjectItem(thriftObject, visibility));
                    AddDocumentEntry addDocumentEntry = new AddDocumentEntry(uri);
                    if (provenanceReg.getParents() != null) {
                        for (InheritanceInfo inheritanceInfo : provenanceReg.getParents()) {
                            addDocumentEntry.addToParents(inheritanceInfo);
                        }
                    }
                    addDocumentEntrySet.add(addDocumentEntry);
                    logger.debug("ADDING to set, uri={}", addDocumentEntry.getUri());

                    if (addDocumentEntrySet.size() >= maxQueueSize) {
                        logger.debug("Hit max queue size - flushing queue");
                        flushQueue();
                    }
                }
            } catch (TException ex) {
                quarantine(ex, thriftObject, visibility);
            } catch (IOException ioe) {
                quarantine(ioe);
            }
        }
    }


    /**
     * Flushes the entries accumulated thus far and writes to the Provenance service.
     *
     * @throws IOException if outputToPipes throws IOException
     */
    private void flushQueue() throws IOException {
        ProvenanceService.Client provenanceClient = null;
        synchronized (this) {
            try {
                provenanceClient = this.pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
                Stopwatch stopwatch = Stopwatch.createStarted();

                Map<String, AddDocumentResult> resultMap;
                resultMap = provenanceClient.addDocuments(this.securityClient.fetchAppToken(this.pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME)),
                                                          addDocumentEntrySet,
                                                          ageOffMappingSet);
                logger.info("Provenanced {} documents | {} ms",
                            addDocumentEntrySet.size(),
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                logger.debug("Sending successfully provenanced data to pipes");

                for (Map.Entry<String, UriToObjectItem> entry : uriToObjectMap.entrySet()) {
                    String uri = entry.getKey();
                    if (!resultMap.containsKey(uri)) {
                        logger.debug("ERROR: URI NOT found in result map: {}", uri);
                        UriToObjectItem uriToObjectItem = entry.getValue();
                        quarantine(uri, uriToObjectItem.object, uriToObjectItem.visibility);
                    } else {
                        final AddDocumentResult addDocumentResult = resultMap.get(uri);
                        AddDocumentStatus status = addDocumentResult.getStatus();

                        UriToObjectItem uriToObjectItem = entry.getValue();
                        if (status == AddDocumentStatus.SUCCESS
                                || (status == AddDocumentStatus.ALREADY_EXISTS && outputDuplicates)) {
                            outputToPipes(uriToObjectItem.visibility, uriToObjectItem.object);
                        } else {
                            logger.debug("NOT sending document to pipes: uri={}", uri);
                            // do not quarantine duplicates
                            if (status != AddDocumentStatus.ALREADY_EXISTS) {
                                quarantine(uri, uriToObjectItem.object, uriToObjectItem.visibility);
                            }
                        }
                    }
                }
                clearArtifacts();

            } catch (TException e) {
                logger.error("Error during batching of documents for Provenance registration.", e);
                throw new IOException(e);
            } finally {
                this.pool.returnToPool(provenanceClient);
            }
        }
    }

    /**
     * This #quarantine is called during #flushQueue if necessary.
     *
     * @param uri        URI for the object being registered.
     * @param object     The object being registered.
     * @param visibility The Accumulo visibility string for this object.
     */
    private void quarantine(String uri, T object, Visibility visibility) {
        AdditionalMetadata metaData = new AdditionalMetadata();

        MetadataEntry urnEntry = new MetadataEntry();
        urnEntry.setValue(uri);
        metaData.putToEntries("uri", urnEntry);

        try {
            sendObjectToQuarantine(object, visibility, "Provenance registration FAILED", metaData);
        } catch (IOException ioe) {
            logger.error("FATAL, cannot send object to Quarantine for uri={}", uri, ioe);
            throw new RuntimeException("Could not send object to Quarantine.", ioe);
        }
    }

    /**
     * This #quarantine is called from #process for an IOException that prevents further processing.
     *
     * @param e the Exception that occurred.
     */
    private void quarantine(Exception e) {
        logger.info("Sending failed ingests to quarantine");
        for (UriToObjectItem uriToObjectItem : uriToObjectMap.values()) {
            quarantine(e, uriToObjectItem.object, uriToObjectItem.visibility);
        }
        clearArtifacts();
    }

    /**
     * This #quarantine is called from #process when a Thrift error occurs.
     * It does not attempt to perform the Thrift conversion again to try to get the URI,
     * since that would most certainly fail again because the conversion had already failed.
     *
     * @param e          the Exception that occurred.
     * @param obj        the object being registered.
     * @param visibility the Accumulo visibility string.
     */
    private void quarantine(Exception e, T obj, Visibility visibility) {
        AdditionalMetadata metaData = new AdditionalMetadata();
        MetadataEntry stackTraceEntry = new MetadataEntry();
        stackTraceEntry.setValue(ExceptionUtils.getFullStackTrace(e));
        metaData.putToEntries("stackTrace", stackTraceEntry);
        MetadataEntry urnEntry = new MetadataEntry();
        logger.info("Sending failed Provenance object to quarantine, stackTrace={}", stackTraceEntry);
        try {
            urnEntry.setValue("IndeterminateURI");
            metaData.putToEntries("uri", urnEntry);
            sendObjectToQuarantine(obj, visibility,"Could not batch register provenance data", metaData);
        } catch (IOException ioe) {
            logger.error("FATAL, cannot send object to Quarantine.", ioe);
            throw new RuntimeException("Could not send object to Quarantine.", ioe);
        }
    }


    private void clearArtifacts() {
        synchronized (this) {
            addDocumentEntrySet.clear();
            ageOffMappingSet.clear();
            uriToObjectMap.clear();
        }
    }

    public final class ProvenanceBatchTask extends TimerTask implements Serializable {
        public ProvenanceBatchTask() {
        }

        @Override
        public void run() {
            logger.debug("Provenance TIMERTASK START period={}ms", period);
            synchronized (this) {
                if (addDocumentEntrySet.size() > 0) {
                    try {
                        logger.debug("Hit batch timeout - flushing queue");
                        flushQueue();
                    } catch (IOException ex) {
                        logger.error("Failed when executing ProvenanceBatchTask", ex);
                        quarantine(ex);
                    }
                }
            }
            logger.debug("Provenance TIMERTASK END");
        }
    }
}



