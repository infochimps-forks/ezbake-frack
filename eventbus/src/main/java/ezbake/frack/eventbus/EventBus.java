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

package ezbake.frack.eventbus;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Envelope;
import ezbake.frack.api.PipelineConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EventBus implements Serializable {
    private Multimap<String, String> topicToHandlers;
    private Map<String, Class<?>> handlerToType;
    private LinkedListMultimap<String, Envelope> workQueue;
    private Map<String, Object> handlerLocks;
    private static final Logger log = LoggerFactory.getLogger(EventBus.class);

    private static final int DEFAULT_QUEUE_MAX = 500;
    private int queueMax;

    public EventBus(Properties props) {
        EzProperties ezProps = new EzProperties(props, true);
        topicToHandlers = Multimaps.synchronizedSetMultimap(HashMultimap.<String, String>create());
        workQueue = LinkedListMultimap.create();
        handlerToType = Maps.newHashMap();
        handlerLocks = Maps.newHashMap();
        queueMax = ezProps.getInteger(PipelineConfiguration.MAX_GENERATOR_QUEUE_SIZE, DEFAULT_QUEUE_MAX);
    }

    /**
     * Add a event handler to the bus and have it listen for a given topic.
     *
     *@param topic the to listen on
     *@param handler the event handler that will handle the work coming in
     */
    public void subscribe(String topic, String handler, Class<?> type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(handler), "A non-null event handler must be passed in!");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "The topic can not be null or empty!");
        topicToHandlers.put(topic, handler);
        if (!handlerToType.containsKey(handler)) {
            handlerToType.put(handler, type);
            handlerLocks.put(handler, new Object());
        }
    }

    /**
     * Publish work to the event bus by a topic
     *
     *@param topic the topic to publish to
     *@param work the work to publish
     */
    public void publish(String topic, Class<?> type, Envelope work) {
        Preconditions.checkNotNull(work, "Work can not be null!");
        Preconditions.checkState(!Strings.isNullOrEmpty(topic), "The topic can not be null or empty!");

        if(!topicToHandlers.containsKey(topic)) {
            log.warn("No handlers found listening on queue for {}", topic);
            return;
        }
        for(String handler : topicToHandlers.get(topic)) {
            if(type.equals(handlerToType.get(handler))) {
                publish(handler, work);
            }
        }
    }


    /**
     * Places work on the queue for the specified handler id and work.
     * @param handlerId the handler id to use
     * @param work the envelope object to place on the queue
     */
    public void publish(String handlerId, Envelope work){
        // Wait until some data has been consumed off the queue so we don't run into OOM errors
        while (queueMax > 0 && workQueue.get(handlerId).size() > queueMax) {
            log.debug("Work queue has reached max size, if you see this message repeatedly please " +
                    "reconfigure the " + PipelineConfiguration.MAX_GENERATOR_QUEUE_SIZE + " parameter to allow for larger work queues.");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Thread interrupted ", e);
            }
        }
        synchronized (handlerLocks.get(handlerId)) {
            workQueue.put(handlerId, work);
            handlerLocks.get(handlerId).notify();
        }
    }

    /**
     * Consume work from the event bus for the given handler.
     *
     * @param handler the ID of the handler that is consuming from the work queue
     */
    public Optional<Envelope> consume(String handler) {
        Preconditions.checkState(!Strings.isNullOrEmpty(handler), "The handler can not be null or empty!");

        Optional<Envelope> result = Optional.absent();
        List<Envelope> handlerQueue = workQueue.get(handler);
        synchronized (handlerLocks.get(handler)) {
            try {
                while (handlerQueue.isEmpty()) {
                    handlerLocks.get(handler).wait();
                }
                result = Optional.of(handlerQueue.remove(0));
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting on work queue lock");
            }
        }
        return result;
    }
}
