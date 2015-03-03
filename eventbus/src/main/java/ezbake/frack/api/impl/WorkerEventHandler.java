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

import com.google.common.base.Optional;
import ezbake.base.thrift.Visibility;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.frack.api.Envelope;
import ezbake.frack.api.Worker;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.EventHandler;
import ezbake.frack.eventbus.util.CopyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class WorkerEventHandler extends EventHandler
{
    private static Logger log = LoggerFactory.getLogger(WorkerEventHandler.class);
    
    public WorkerEventHandler(EventBus bus, String workerId, Worker worker, String pipelineId, Properties props, int numThreads)
    {
        for (int i = 0; i < numThreads; i++) {
            Worker copiedWorker = CopyHelper.deepCopyObject(worker);
            threads.add(new WorkerThread(bus, workerId, copiedWorker, pipelineId, props));
        }
    }

    static class WorkerThread extends EventHandlerThread {
        private EventBus bus;
        private Worker worker;
        private String topic;
        private String pipelineId;
        private Properties props;

        public WorkerThread(EventBus bus, String topic, Worker worker, String pipelineId, Properties props) {
            this.bus = bus;
            this.worker = worker;
            this.topic = topic;
            this.pipelineId = pipelineId;
            this.props = props;

            EzBroadcaster broadcaster = null;
            if (!worker.getBroadcastTopics().isEmpty()) {
                broadcaster = EzBroadcaster.create(props, PipelineContextProvider.get().getPipelineId());

                for (String broadcastTopic : (Set<String>) worker.getBroadcastTopics()) {
                    broadcaster.registerBroadcastTopic(broadcastTopic);
                }
            }
            this.worker.registerWorkerPublisher(new EventBusWorkerPublisher(props, bus, topic, pipelineId, broadcaster));
        }

        @Override
        public void run() {
            worker.setup(props, pipelineId);
            while(!done) {
                Optional<Envelope> opWork = bus.consume(topic);
                if (opWork.isPresent()) {

                    byte[] work = opWork.get().getData();
                    Visibility visibility = opWork.get().getVisibility();
                    try {
                        worker.doWork(visibility, work);
                    } catch (IOException e) {
                        log.error("Error while attempting to process work", e);
                    }
                }
            }
        }
    }
}
