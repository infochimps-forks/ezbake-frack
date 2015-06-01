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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.warehaus.IngestStatus;
import ezbake.warehaus.IngestStatusEnum;
import ezbake.warehaus.Repository;
import ezbake.warehaus.WarehausService;
import ezbake.warehaus.WarehausServiceConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * A concrete generic Worker class that sends a Thrift-based object to the Data Warehouse service.
 * This class requires two {@link ezbake.frack.common.workers.IThriftConverter} collaborators that convert your
 * Thrift object into types required for storage in the data warehaus:
 * <ul>
 * <li>{@link ezbake.warehaus.Repository}</li>
 * <li>{@link ezbake.base.thrift.DocumentClassification}</li>
 * </ul>
 *
 * @param <T> The class type of the object that is being stored.
 */
public class WarehausWorker<T extends Serializable> extends Worker<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(WarehausWorker.class);

    /**
     * This is the suffix to use when registering your worker in a pipeline whose value is {@value}.
     *
     *<pre>
     * {@code
     * Pipeline pipeline = new Pipeline();
     * Worker<Document> worker =
     *      new WarehausWorker<Document>(Document.class,
     *                                  new DocumentToRepositoryConverter(),
     *                                  new DocumentToDocumentClassificationConverter());
     * String workerId = pipeline.getId() + WarehausWorker.PIPELINE_SUFFIX;
     * pipeline.addWorker(workerId, worker);
     * }
     *</pre>
     */
    @SuppressWarnings("unused")
    public static final String PIPELINE_SUFFIX = "_warehaus_worker";
    private final IThriftConverter<T, Repository> fromObjectToRepository;
    private final IThriftConverter<T, Visibility> fromObjectToVisibility;
    protected ThriftClientPool pool;
    protected EzbakeSecurityClient securityClient;
    protected String warehausSecurityId;

    /**
     * Creates a WarehausWorker object.
     * @param thriftObject The class type of the Thrift-based object to be sent to the Data Warehouse.
     * @param fromObjectToRepository An instance of a Thrift Converter class that converts a Thrift object into a Repository object.
     * @param fromObjectToVisibility An instance of a Thrift Converter class that converts a Thrift object to a Visibility object.
     */
    public WarehausWorker(Class<T> thriftObject,
                          IThriftConverter<T, Repository> fromObjectToRepository,
                          IThriftConverter<T, Visibility> fromObjectToVisibility) {
        super(thriftObject);
        this.fromObjectToRepository = fromObjectToRepository;
        this.fromObjectToVisibility = fromObjectToVisibility;
    }

    /**
     * Initialization method that uses configuration values specific to the implementing Pipe.
     * @param props the configuration for the Pipeline
     */
    @Override
    public void initialize(Properties props) {
        this.pool = new ThriftClientPool(props);
        this.securityClient = new EzbakeSecurityClient(props);
        this.warehausSecurityId = this.pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME);
    }

    /**
     * This method sends the converted Thrift Repository object to the Data Warehouse service.
     * @param visibility The Accumulo visibility string representing the classification level of the data contained in the incoming thrift object
     * @param thriftObject  The incoming Thrift object to be processed.
     */
    @Override
    public void process(Visibility visibility, T thriftObject) {
        WarehausService.Client warehaus = null;
        IngestStatus status = null;
        try {
            warehaus = this.pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);
            EzSecurityToken securityToken = securityClient.fetchAppToken(warehausSecurityId);
            Repository repository = fromObjectToRepository.convert(thriftObject);
            Visibility documentVisibility = fromObjectToVisibility.convert(thriftObject);
            logger.info("inserting {} into warehouse with visibility {}", repository.getUri(), documentVisibility);
            status = warehaus.insert(repository, documentVisibility, securityToken);
            this.pool.returnToPool(warehaus);
            warehaus = null;
            if (status != null && status.getStatus() != IngestStatusEnum.SUCCESS) {
                logger.error("Failed to successfully ingest object to warehouse. IngestStatus {}", status);
                throw new IOException("Failed to successfully ingest object to warehouse.");
            }
            outputToPipes(documentVisibility, thriftObject);
        } catch (IOException | TException e) {
            logger.warn("Tried to send document to warehaus: {}", e);
            if (warehaus != null) {
                this.pool.returnBrokenToPool(warehaus);
            }
            AdditionalMetadata metadata = new AdditionalMetadata();
            metadata.putToEntries("stacktrace", new MetadataEntry().setValue(StackTraceUtil.getStackTrace(e)));
            
            if (status != null) {
                metadata.putToEntries("ingestStatus", new MetadataEntry().setValue(status.getStatus().toString()));
                if (!status.getFailedURIs().isEmpty()) {
                    metadata.putToEntries("uri", new MetadataEntry().setValue(status.getFailedURIs().get(0)));
                }
                if (StringUtils.isNotEmpty(status.getFailureReason())) {
                    metadata.putToEntries("failureReason", new MetadataEntry().setValue(status.getFailureReason()));
                }
            }
            
            try {
                sendObjectToQuarantine(thriftObject, visibility, e.getMessage(), metadata);
            } catch (IOException e1) {
                logger.error("FATAL, cannot send object to Quarantine.", e1);
                throw new RuntimeException("Could not send object to Quarantine.", e1);
            }
        }
    }
}

