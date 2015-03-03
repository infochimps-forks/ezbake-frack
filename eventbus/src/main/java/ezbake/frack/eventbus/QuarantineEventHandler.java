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

import ezbake.frack.api.Envelope;
import ezbake.frack.eventbus.util.QuarantineProcessor;
import ezbake.quarantine.thrift.ObjectNotQuarantinedException;
import ezbake.quarantine.thrift.ObjectStatus;
import ezbake.quarantine.thrift.QuarantineResult;
import ezbake.quarantine.thrift.QuarantinedObject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

public class QuarantineEventHandler extends EventHandler {
    private static final Logger log = LoggerFactory.getLogger(QuarantineEventHandler.class);
    private static final int DEFAULT_SLEEP_MS = 180000;


    public QuarantineEventHandler(EventBus bus, Properties props, String pipelineId){
        threads.add(new QuarantineHandlerThread(bus, props, pipelineId));
    }

    /**
     * This class handles replaying quarantine objects.
     */
    static class QuarantineHandlerThread extends EventHandlerThread{
        private QuarantineProcessor processor;
        private EventBus bus;
        private String pipelineId;

        public QuarantineHandlerThread(EventBus bus, Properties props, String pipelineId) {
            this.bus = bus;
            this.processor = new QuarantineProcessor(props, pipelineId);
            this.pipelineId = pipelineId;
        }

        @Override
        public void run() {
            while(!done){
                try {
                    generate();
                } catch (IOException e) {
                    log.error("Error while processing quarantine data, some data may have been dropped", e);
                }
            }
        }

        public void generate() throws IOException {
            try {
                log.debug("Entered QuarantineGenerator generate! getting objects now");
                String updateMessage = "Status updated to %s by QuarantineSpout.";

                // Continue calling the Quarantine service until an ObjectNotQuarantined exception is thrown, which means
                // that all of the objects have been reingested.
                while(true) {
                    List<QuarantineResult> resultList = processor.getApprovedObjectsForPipeline(pipelineId, 0, (short)50);

                    for (QuarantineResult result : resultList) {
                        QuarantinedObject obj = result.getObject();
                        Envelope envelope =  new Envelope(obj.getVisibility(), obj.getContent());
                        processor.updateStatus(result.getId(), ObjectStatus.PREPARED_TO_REINGEST, String.format(updateMessage, ObjectStatus.PREPARED_TO_REINGEST));
                        bus.publish(obj.getPipeId(), envelope);
                        processor.updateStatus(result.getId(), ObjectStatus.REINGESTED, String.format(updateMessage, ObjectStatus.REINGESTED));
                    }
                }
            } catch (ObjectNotQuarantinedException e) {
                log.debug("Reingested all Quarantined data");
            } catch (TException e) {
                log.error("Could not retrieve items from Quarantine", e);
            } finally {
                try {
                    Thread.sleep(DEFAULT_SLEEP_MS);
                } catch (InterruptedException e) {
                    log.error("Thread interrupted", e);
                }
            }
        }
    }
}
