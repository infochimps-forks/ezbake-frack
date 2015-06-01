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

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * A generic Worker that broadcasts Thrift objects to a collection of Data Topics.
 *
 * This class requires an {@link ezbake.frack.common.workers.IThriftConverter} collaborator that converts your
 * Thrift object into something suitable for broadcasting.
 * If your thrift object is greater than 1M you do not want to put it onto a Kafka queue without transforming
 * it to a more manageable size with a converter.
 * In other cases (size < 1M), your Thrift object is suitable for broadcasting directly
 * so you can simply use the {@link ezbake.frack.common.workers.PassThruThriftConverter}.
 *
 * @param <T> The class type of the Thrift object to be broadcast.
 */
public class BroadcastWorker<T extends Serializable> extends Worker<T> {
    private static final long serialVersionUID = 1L;
    /**
     * This is the suffix to use when registering your worker in a pipeline whose value is {@value}.
     * For example:
     * <pre>
     * {@code
     * Pipeline pipeline = new Pipeline();
     * Worker<Article> worker =
     *     new BroadcastWorker<Article>(Article.class,
     *                                  newHashSet("Topic1", "Topic2"),
     *                                  new BroadcastConverter());
     * String workerId = pipeline.getId() + BroadcastWorker.PIPELINE_SUFFIX;
     * pipeline.addWorker(workerId, worker);
     * }
     * </pre>
     */
    public static final String PIPELINE_SUFFIX = "_broadcast_worker";
    private final static Logger logger = LoggerFactory.getLogger(BroadcastWorker.class);
    private final IThriftConverter<T, ? extends TBase> converter;

    /**
     * Create an instance of this class.
     * @param thriftObjectType the incoming Thrift object's class type.
     * @param topics the topics to broadcast to.
     * @param converter object that transforms an instance of {@code thriftObjectType} to the Thrift object to be broadcast.
     */
    public BroadcastWorker(Class<T> thriftObjectType, Set<String> topics, IThriftConverter<T, ? extends TBase> converter) {
        super(thriftObjectType);
        this.converter = converter;
        for(String topic : topics)
            this.registerBroadcastTopic(topic);
    }

    /**
     * Getter method
     * @return The instance of the converter.
     */
    public IThriftConverter<T, ? extends TBase> getConverter() {
        return converter;
    }

    /**
     * Broadcasts the Thrift object to the collection of Data Topics.
     * @param visibility The visibility representing what users/applications are able to see the object
     * @param object The object to be converted to a Thrift struct and then broadcast.
     * @throws IOException
     */
    @Override
    public void process(Visibility visibility, T object) {
        logger.debug("processing a [{}]", getType().getName());
        try {
            broadcastTopics(visibility, object);
        } catch (IOException e) {
            logger.warn("Tried to broadcast message {}", e);
            AdditionalMetadata metadata = new AdditionalMetadata();
            metadata.putToEntries("stacktrace", new MetadataEntry().setValue(StackTraceUtil.getStackTrace(e)));
            try {
                sendObjectToQuarantine(object, visibility, e.getMessage(), metadata);
            } catch (IOException e1) {
                logger.error("FATAL, cannot send object to Quarantine.", e1);
                throw new RuntimeException("Could not send object to Quarantine.", e1);
            }
        }
    }

    private void broadcastTopics(Visibility visibility, T object) throws IOException {
        if(getBroadcastTopics().isEmpty()) return;
        try {
            TBase thriftToBroadcast = getConverter().convert(object);
            for(String topic : getBroadcastTopics()) {
                logger.info("Broadcast to the {} topic with {} visibility", topic, visibility);
                broadcast(topic, visibility, thriftToBroadcast);
            }
        } catch (TException e) {
            throw new IOException("Could not convert thrift object.", e);
        }
    }
}
