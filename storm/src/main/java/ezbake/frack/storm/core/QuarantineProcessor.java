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

package ezbake.frack.storm.core;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.quarantine.client.QuarantineClient;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.QuarantineConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;


public class QuarantineProcessor extends QuarantineClient {
    private static final Logger logger = LoggerFactory.getLogger(QuarantineProcessor.class);
    private String               pipelineId;
    private String               quarantineSecurityId;

    /**
     * Creates a quarantine processor object with provided configuration.
     * One quarantine processor object should be created per pipeline.
     * @param props pipeline specific configurations to use.
     * @param pipelineId id of the pipeline
     */
    public QuarantineProcessor(Properties props, String pipelineId) {
        super(props);

        try {
            this.pipelineId = pipelineId;
            this.quarantineSecurityId = pool.getSecurityId(QuarantineConstants.SERVICE_NAME);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Quarantines raw data.  This method is visible in both worker and generator.
     * The raw data is NOT automatically replayed.
     * @param pipeId the id of the pipe where the data came from (could be generator or worker id)
     * @param data the raw data to save
     * @param visibility string identifying the visibility of the this data.
     * @param error a string describing the reason for failure
     */
    protected void sendRawToQuarantine(String pipeId, byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        sendRawToQuarantine(pipelineId, pipeId, data, visibility, error, additionalMetadata);
    }

    /**
     * Quarantines serializable data.  This method is visible in both worker and generator class.
     * This data is automatically replayed after approval.
     * @param pipeId the id of the pipe where the data came from (could be generator or worker id)
     * @param object the Serializable object to save
     * @param visibility string identifying the visibility of the this data.
     * @param error a string describing the reason for failure
     */
    protected void sendObjectToQuarantine(String pipeId, Serializable object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        sendObjectToQuarantine(pipelineId, pipeId, object, visibility, error, additionalMetadata);
    }

    @Override
    protected EzSecurityToken getToken() throws IOException {
        try {
            return client.fetchAppToken(quarantineSecurityId);
        } catch (EzSecurityTokenException e) {
            throw new IOException("Could not retrieve security token from ezbake security", e);
        }
    }
}
