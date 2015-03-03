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

import ezbake.ezbroadcast.core.Receiver;
import ezbake.ezbroadcast.core.thrift.SecureMessage;
import ezbake.frack.api.Envelope;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class StormReceiver implements Receiver {
    private static final Logger log = LoggerFactory.getLogger(StormReceiver.class);
    private SpoutPublisher publisher;
    private Class<? extends TBase> type;

    public StormReceiver(SpoutPublisher publisher, Class<? extends TBase> type) {
        this.publisher = publisher;
        this.type = type;
    }

    @Override
    public void receive(String topic, SecureMessage message) throws IOException {
        // When we receive a message from the message bus, simply push it out to Storm's queue
        // Object needs to be deserialized from thrift, then serialized through Java serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(ThriftUtils.deserialize(type, message.getContent()));
        } catch (TException e) {
            log.error(String.format("Could not deserialize object from topic %s", topic), e);
            throw new IOException(e);
        } finally {
            oos.flush();
            oos.close();
        }
        publisher.publish(type, new Envelope(message.getVisibility(), baos.toByteArray()));
    }
}