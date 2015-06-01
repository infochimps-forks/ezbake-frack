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

import com.google.common.collect.Lists;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.*;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.EventHandler;
import ezbake.frack.eventbus.QuarantineEventHandler;
import ezbake.frack.eventbus.util.ConfigurationHelper;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventBusPipelineSubmitter implements PipelineSubmitter {
    private static Logger log = LoggerFactory.getLogger(EventBusPipelineSubmitter.class);
    List<EventHandler> handlers = Lists.newArrayList();
    ExecutorService service;

    @Override
    public void submit(final Pipeline pipeline) throws IOException {
        int totalThreads = 0;
        Properties props = pipeline.getProperties();
        EventBus bus = new EventBus(props);
        String pipelineId = pipeline.getId();
        Map<String, Class<? extends TBase>> pipeMap = new HashMap<>();

        log.info("Submitting pipeline: {}", pipelineId);

        for (Pipeline.PipeInfo pipeInfo : pipeline.getPipeInfos()) {
            String pipeId = pipeInfo.getPipeId();
            Pipe pipe = pipeInfo.getPipe();
            int pipeThreads = Math.max(ConfigurationHelper.getNumExecutors(props, pipeId),
                                        ConfigurationHelper.getNumTasks(props, pipeId));
            totalThreads += pipeThreads;
            log.info("Initializing {} threads for {}", pipeThreads, pipeId);

            if (Pipeline.isGenerator(pipe)) {
                handlers.add(new GeneratorEventHandler(bus, pipeId, (Generator)pipe, pipelineId, props, pipeThreads));
            } else if (Pipeline.isWorker(pipe)) {

                Worker worker = (Worker)pipe;
                for (String pipeToSubscribeTo : pipeInfo.getInputs()) {
                    bus.subscribe(pipeToSubscribeTo, pipeId, worker.getType());
                }
                pipeMap.put(pipeId, worker.getType());
                handlers.add(new WorkerEventHandler(bus, pipeId, worker, pipelineId, props, pipeThreads));
            } else if (Pipeline.isListener(pipe)) {
                handlers.add(new ListenerEventHandler(bus, pipeId, (Listener)pipe, pipelineId, props, pipeThreads));
            }
        }

        boolean quarantineEnabled = new EzProperties(props, true).getBoolean(PipelineConfiguration.ENABLE_QUARANTINE, false);
        if (quarantineEnabled) {
            //Quarantine event handler is used for processing quarantined objects on disk.
            //One instance of this handler should be created per pipeline.
            QuarantineEventHandler quarantineHandler = new QuarantineEventHandler(bus, props, pipelineId);
            handlers.add(quarantineHandler);

            //One additional thread is needed for quarantine event handler
            totalThreads++;
        }

        service = Executors.newFixedThreadPool(totalThreads);

        log.info("Starting up threads...");
        for (EventHandler handler : handlers) {
            for (EventHandler.EventHandlerThread thread : handler.getThreads()) {
                service.execute(thread);
            }
        }
        log.info("Pipeline started!");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                PipelineContext context = new PipelineContext();
                context.setProps(pipeline.getProperties());
                context.setPipelineId(pipeline.getId());
                PipelineContextProvider.add(context);
                for (EventHandler handler : handlers) {
                    handler.shutdown();
                }
            }
        }));
    }
}
