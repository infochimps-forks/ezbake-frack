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

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * This interface defines methods to interact with backend data stores
 * for fault tolerance.
 */
public interface FrackLedger extends Serializable {

	/**
     * Inserts the given data to the underlying transaction ledger. 
     *
     * @param messageId the messageId
     * @param fromPipe the pipe's pipeId
     * @param toPipe the pipe's pipeId
     * @param visibility the visibility of the entry
     * @param message the data to be aded
     */
    public void insert(String messageId, String fromPipe, String toPipe, Visibility visibility, byte[] message) throws IOException;
    
    /**
     * Indicates whether ledger has messages for pipelineId supplied. </br > 
     *
     * @param pipelineId the pipelineId
     * @param timestamp the timestamp of the ending entry
     * @return Returns {@literal ConcurrentHashMap<String, String>} of messageId and visibility
     */
    public boolean hasQueuedItems(String pipelineId, long timestamp) throws IOException;
    
    /**
     * Gets an entry from the underlying transaction ledger. 
     *
     * @param messageId the messageId
     * @param fromPipe the pipe's pipeId
     * @param toPipe the pipe's pipeId
     * @param visibility the visibility of the entry
     * @return data as byte[]
     */
    public byte[] get(String messageId, String fromPipe, String toPipe, Visibility visibility) throws IOException;
    
    /**
     * Gets a list of queued messages for pipeId supplied. </br >
     * Items retrieved from the underlying transaction ledger. 
     *
     * @param fromPipe the pipe's pipeId
     * @param toPipe the pipe's pipeId
     * @param timestamp the timestamp of the ending entry
     * @return Returns {@literal ConcurrentHashMap<String, String>} of messageId and visibility
     */
    public Map<String, Visibility> getQueuedItems(String fromPipe, String toPipe, long timestamp) throws IOException;
    
    /**
     * Deletes an entry from the underlying transaction ledger. 
     *     
     * @param messageId the messageId
     * @param fromPipe the pipe's pipeId
     * @param toPipe the pipe's pipeId
     * @param visibility the visibility of the entry
     */
    public void delete(String messageId, String fromPipe, String toPipe, Visibility visibility) throws IOException;
    
}
