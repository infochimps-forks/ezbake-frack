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

package ezbake.frack.api;

import ezbake.base.thrift.Visibility;
import ezbake.quarantine.thrift.AdditionalMetadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * FOR INTERNAL USE ONLY. This class is used to publish data between Pipes to
 * the underlying ingest system.
 */
public interface Publisher extends Serializable {

    /**
     * Publishes the given Envelope to the underlying ingest system.
     *
     * @param clazz the type of data being published
     * @param envelope the data to be published
     */
    public void publish(Class<? extends Serializable> clazz, Envelope envelope) throws IOException;

    /**
     * Sends the raw data to the quarantine service.
     *
     * This data will not be automatically replayed since there
     * is no way to determine the type of the data.
     *
     * @param data raw data to quarantine
     * @param visibility visibility for this data
     * @param error a string that should be used to categorize a problem, for example "Could not index data because
     *              the Warehouse service is down". All quarantined objects with the same error string will be
     *              correlated.
     * @param additionalMetadata any additional metadata that would be useful in debugging or characterizing this issue
     */
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException;
}
