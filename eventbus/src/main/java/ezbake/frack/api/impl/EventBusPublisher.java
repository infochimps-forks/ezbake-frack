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

package ezbake.frack.api.impl;

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Envelope;
import ezbake.frack.api.Publisher;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.util.QuarantineProcessor;
import ezbake.quarantine.thrift.AdditionalMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;


public class EventBusPublisher implements Publisher {
    protected EventBus bus;
    protected String sourcePipe;
    protected String sourcePipeline;
    protected QuarantineProcessor processor;
    protected static final Logger log = LoggerFactory.getLogger(EventBusPublisher.class);

    public EventBusPublisher(Properties props, EventBus bus, String sourcePipeline, String sourcePipe) {
        this.bus = bus;
        this.sourcePipeline = sourcePipeline;
        this.sourcePipe = sourcePipe;
        this.processor = new QuarantineProcessor(props, sourcePipeline);
    }

    @Override
    public void publish(Class<? extends Serializable> aClass, Envelope envelope) {
        log.debug("Publishing to topic " + sourcePipe);
        bus.publish(sourcePipe, aClass, envelope);
    }

    @Override
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        log.debug("Sending to raw to quarantine " + sourcePipe);
        processor.sendRawToQuarantine(sourcePipe, data, visibility, error, additionalMetadata);
    }
}
