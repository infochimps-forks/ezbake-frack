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
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.frack.api.WorkerPublisher;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.util.QuarantineProcessor;
import ezbake.quarantine.thrift.AdditionalMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class EventBusWorkerPublisher extends EventBusPublisher implements WorkerPublisher {
    private EzBroadcaster broadcaster;
    private static final Logger log = LoggerFactory.getLogger(EventBusWorkerPublisher.class);

    public EventBusWorkerPublisher(Properties props, EventBus bus, String sourcePipe, String pipelineId, EzBroadcaster broadcaster) {
        super(props, bus, pipelineId, sourcePipe);
        this.broadcaster = broadcaster;
    }

    @Override
    public void broadcast(String topic, Visibility visibility, byte[] message) throws IOException {
        if (broadcaster != null) {
            broadcaster.broadcast(topic, visibility, message);
        } else {
            log.error("No topics were registered for this worker!");
            throw new RuntimeException("Must register topics with worker before broadcasting to them");
        }
    }

    @Override
    public void sendObjectToQuarantine(Serializable object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        log.debug("Sending to quarantine " + sourcePipe);
        processor.sendObjectToQuarantine(sourcePipe, object, visibility, error, additionalMetadata);
    }
}
