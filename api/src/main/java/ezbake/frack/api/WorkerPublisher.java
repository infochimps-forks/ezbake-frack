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
 * Created by eperry on 2/12/14.
 */
public interface WorkerPublisher<T extends Serializable> extends Publisher {
    /**
     * Broadcasts the given data to the underlying message bus. This method
     * is only used within the context of a worker.
     *
     * @param topic the topic to which the struct is being broadcast
     * @param visibility the visibility of the struct being broadcast
     * @param message the data to be broadcast
     */
    public void broadcast(String topic, Visibility visibility, byte[] message) throws IOException;

    /**
     * Sends the serializable object to quarantine.
     *
     * @param object serializable object
     * @param visibility string specifying the visibility of this data
     * @param error a string that should be used to categorize a problem, for example "Could not index data because
     *              the Warehouse service is down". All quarantined objects with the same error string will be
     *              correlated.
     * @param additionalMetadata any additional metadata that would be useful in debugging or characterizing this issue
     */
    public void sendObjectToQuarantine(T object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException;
}
