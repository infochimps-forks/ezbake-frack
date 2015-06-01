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

import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.frack.api.Listener;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.EventHandler;
import ezbake.frack.eventbus.util.CopyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class ListenerEventHandler extends EventHandler
{
    private static Logger log = LoggerFactory.getLogger(ListenerEventHandler.class);

    static class ListenerThread extends EventHandlerThread
    {
        private Listener listener;
        private EzBroadcaster broadcaster;
        private String pipelineId;
        private Properties props;
        private EventBusPublisher publisher;

        public ListenerThread(Listener listener, String pipeId, EventBus bus, String pipelineId, Properties props) {
            this.broadcaster = EzBroadcaster.create(props, PipelineContextProvider.get().getPipelineId());
            for (String topic : (Set<String>)listener.getListenerTopics()) {
                broadcaster.subscribeToTopic(topic);
            }
            this.props = props;
            this.pipelineId = pipelineId;
            this.listener = listener;
            this.publisher = new EventBusPublisher(props, bus, pipelineId, pipeId);
        }

        @Override
        public void run()
        {
            listener.setup(props, pipelineId);
            broadcaster.startListening(new EventBusReceiver(listener.getType(), publisher));
            while(!done);
            try {
                broadcaster.close();
            } catch (IOException e) {
                log.error("Error closing broadcaster", e);
            }
        }
    }

    public ListenerEventHandler(EventBus bus, String pipeId, Listener listener, String pipelineId, Properties props, int numThreads)
    {
        for (int i = 0; i < numThreads; i++) {
            threads.add(new ListenerThread(CopyHelper.deepCopyObject(listener), pipeId, bus, pipelineId, props));
        }
    }
}
