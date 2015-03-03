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

import com.google.common.collect.Sets;
import ezbake.base.thrift.Visibility;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Set;

/**
 * Worker class which takes a Thrift object as input and performs processing on it.
 * Implementers must implement one method which receives and processes a Serializable object
 * which they define. The {@link Worker#outputToPipes(Visibility, java.io.Serializable)} and
 * {@link Worker#broadcast(String, Visibility, org.apache.thrift.TBase)} methods can then be used to push
 * transformed objects to other Pumps, or onto a message queue for processing by another
 * Pipeline.
 *
 * @param <T> the class being processed by the Worker.
 */
public abstract class Worker<T extends Serializable> extends Pipe<Serializable> implements Serializable
{
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private Class<T> type;
    private Set<String> broadcastTopics;
    private WorkerPublisher workerPublisher;

    public Worker(Class<T> type)
    {
        this.type = type;
        this.broadcastTopics = Sets.newHashSet();
    }

    /**
     * This method takes the user defined Thrift object class as input, and performs whatever
     * processing the implementer needs.
     *
     * @param visibility the Accumulo visibility string representing the classification level of
     *                   the data contained in the incoming thrift object
     * @param object Incoming Thrift object to be processed
     */
    public abstract void process(Visibility visibility, T object);

    /**
     * This method broadcasts a Thrift object onto a message queue for the given topic. The
     * topic must be registered before calling broadcast using the {@link Worker#registerBroadcastTopic(String)}
     * method. This should be called within {@link Worker#process(Visibility, java.io.Serializable)}.
     *
     * @param topic the topic on which to broadcast the object
     * @param visibility the Accumulo visibility string representing the classification level of
     *                   the data to be broadcast
     * @param t the thrift object to be broadcast
     * @throws IOException
     */
    protected final void broadcast(String topic, Visibility visibility, TBase t) throws IOException {
        try {
            workerPublisher.broadcast(topic, visibility, ThriftUtils.serialize(t));
        } catch (TException e) {
            log.error(String.format("TException occurred when attempting to broadcast to topic %s", topic), e);
            throw new IOException(e);
        }
    }

    /**
     * This method registers the given topic for broadcasting with the underlying message queue.
     *
     * @param topic topic to register for broadcasting with this Worker
     */
    public final void registerBroadcastTopic(String topic) {
        log.debug("Registering worker for broadcast topic '{}'", topic);
        broadcastTopics.add(topic);
    }

    /**
     * This method returns all topics that this Worker is registered for broadcasting.
     *
     * @return the Set of topics to which this Worker can broadcast
     */
    public final Set<String> getBroadcastTopics() {
        return broadcastTopics;
    }

    /**
      * FOR USE ONLY BY UNDERLYING IMPLEMENTATION
      */
    public final Class<T> getType() {
        return type;
    }

    /**
      * FOR USE ONLY BY UNDERLYING IMPLEMENTATION
      */
    public final void doWork(Visibility visibility, byte[] bytes) throws IOException {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            T object = (T) ois.readObject();
            ois.close();
            process(visibility, object);
        } catch (ClassNotFoundException e) {
            log.error("Could not deserialize class because it could not be found, fatal error.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * FOR USE ONLY BY UNDERLYING IMPLEMENTATION
     */
    public void registerWorkerPublisher(WorkerPublisher p) {
        super.registerPublisher(p);
        this.workerPublisher = p;
    }

    /**
     * This method sends the given thrift object to quarantine.  It should be invoked
     * on failure.
     *
     * @param object serializable object
     * @param visibility specifies access control for this data
     * @param error a string that should be used to categorize a problem, for example "Could not index data because
     *              the Warehouse service is down". All quarantined objects with the same error string will be
     *              correlated.
     * @param additionalMetadata any additional metadata that would be useful in debugging or characterizing this issue
     * @throws java.io.IOException if the provided object could not be quarantined
     */
    public void sendObjectToQuarantine(T object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        log.debug("workerpublisher.class - " + workerPublisher.getClass().getName() + " package: " + workerPublisher.getClass().getPackage());
        workerPublisher.sendObjectToQuarantine(object, visibility, error, additionalMetadata);
    }
}
