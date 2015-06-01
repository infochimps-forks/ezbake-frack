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
import java.util.Properties;

import ezbake.services.provenance.thrift.ProvenanceDocumentExistsException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.thrift.ProvenanceRegistration;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.thrift.ThriftClientPool;

/**
 * <p>
 * A Worker class that registers an ingested document with the Provenance
 * service.
 * </p>
 *
 * @param <T> The class type of the document being registered with the
 *            Provenance service.
 */
@SuppressWarnings("unused")
public class ProvenanceWorker<T extends Serializable> extends Worker<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceWorker.class);

    @SuppressWarnings("unused")
    public static final String PIPELINE_SUFFIX = "_provenance_worker";

    private final IThriftConverter<T, ProvenanceRegistration> itcProvenanceReg;
    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;

    /**
     * <p>
     * Construct an instance of this class.
     * </p>
     *
     * @param type             The type of thrift object that will be passed into the
     *                         worker. Required.
     * @param itcProvenanceReg The Thrift converter that will convert thrift
     *                         object instances processed by this worker and turn them into
     *                         instances of {@link ezbake.frack.common.utils.thrift.ProvenanceRegistration}.
     */
    public ProvenanceWorker(Class<T> type, IThriftConverter<T, ProvenanceRegistration> itcProvenanceReg) {

        super(type);
        this.itcProvenanceReg = itcProvenanceReg;
    }

    /**
     * <p>
     * Initializes the worker with configuration and actions needed prior to
     * processing.
     * </p>
     *
     * @param properties The application properties associated with the
     *                   environment.
     */
    public void initialize(Properties properties) {

        this.pool = new ThriftClientPool(properties);
        this.securityClient = new EzbakeSecurityClient(properties);
    }

    /**
     * <p>
     * Process the document add to the provenance service.
     * </p>
     *
     * @param visibility   The Accumulo visibility string representing the
     *                     classification level of the data contained in the incoming thrift object.
     * @param thriftObject The incoming Thrift object to be processed.
     */
    @Override
    public void process(Visibility visibility, T thriftObject) {

        ProvenanceService.Client provenanceClient = null;
        try {
            ProvenanceRegistration provenanceReg = this.itcProvenanceReg.convert(thriftObject);
            provenanceClient = this.pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
            long provenanceId = 0;
            try {
                provenanceId = provenanceClient.addDocument(
                        this.securityClient.fetchAppToken(this.pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME)),
                        provenanceReg.getUri(),
                        provenanceReg.getParents(),
                        provenanceReg.getAgeOffRules());
            } catch (ProvenanceDocumentExistsException e) {
                logger.warn("This document is already in the Provenance service.", e);
            }
            logger.debug("Provenance Service Id for URI {}: {}", provenanceReg.getUri(), provenanceId);
            this.outputToPipes(visibility, thriftObject);
            this.pool.returnToPool(provenanceClient);
        } catch (TException | IOException e) {
            logger.error("An error occurred while accessing the Provenance service.", e);
            this.pool.returnBrokenToPool(provenanceClient);
            throw new RuntimeException(e);
        }
    }

}
