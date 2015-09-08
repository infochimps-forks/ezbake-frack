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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Output<T extends Serializable> implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Output.class);

    private Publisher publisher = null;

    protected void registerPublisher(Publisher p) {
        if (publisher == null) {
            publisher = p;
        } else {
            throw new RuntimeException("A publisher was already registered for this Pipe, cannot register a publisher twice");
        }
    }

    public void toPipes(Visibility visibility, T object) throws IOException {
	log.trace("sending {} to pipes", object);

        if (publisher == null) {
            throw new RuntimeException("A publisher must be registered to outputToPipes");
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.flush();
        oos.close();
        publisher.publish(object.getClass(), new Envelope(visibility, baos.toByteArray()));
    }


    public void rawToQuarantine(byte[] rawData, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        if(publisher == null){
            throw new RuntimeException("A publisher must be registered");
        }
        publisher.sendRawToQuarantine(rawData, visibility, error, additionalMetadata);
    }
}
