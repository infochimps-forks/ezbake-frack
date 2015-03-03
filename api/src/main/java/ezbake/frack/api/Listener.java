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
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

/**
 * This class represents a data source that listens to the given topics and distributes the
 * received objects to all connected Pipes. This can be used in any Pipeline in which the
 * desired data is coming from an outside source which broadcasts onto a specific topic.
 *
 * @param <I> the type of object that is coming in from the message queue, and also being
 *            distributed to the connected Pipes
 */
public final class Listener<I extends TBase> extends Pipe<I> implements Serializable
{
    private static final Logger log = LoggerFactory.getLogger(Listener.class);
    private Class<I> type;
    private Set<String> topics;

    public Listener(Class<I> type) {
        this.type = type;
        this.topics = Sets.newHashSet();
    }

    public final void registerListenerTopic(String topic) {
        log.debug("Registering to listen to topic '{}'", topic);
        topics.add(topic);
    }

    public final Set<String> getListenerTopics() {
        return topics;
    }

    public final Class<I> getType() {
        return type;
    }
}
