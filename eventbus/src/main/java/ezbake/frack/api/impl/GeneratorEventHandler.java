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

import ezbake.frack.api.Generator;
import ezbake.frack.eventbus.EventBus;
import ezbake.frack.eventbus.EventHandler;
import ezbake.frack.eventbus.util.CopyHelper;

import java.util.Properties;

public class GeneratorEventHandler extends EventHandler
{
    static class GeneratorThread extends EventHandlerThread
    {
        private Generator generator;
        private String pipelineId;
        private Properties props;

        public GeneratorThread(Generator generator, EventBus bus, String pipelineId, String pipeId, Properties props)
        {
            //super(config, pipelineId, generator);
            this.generator = generator;
            this.props = props;
            this.pipelineId = pipelineId;
            this.generator.registerPublisher(new EventBusPublisher(props, bus, pipelineId, pipeId));
        }

        @Override
        public void run()
        {
            generator.setup(props, pipelineId);
            while(!done)
            {
                generator.generate();
            }
        }
    }

    public GeneratorEventHandler(EventBus bus, String pipeId, Generator generator, String pipelineId,
                                 Properties props, int numThreads)
    {
        for (int i = 0; i < numThreads; i++) {
            Generator copiedGenerator = CopyHelper.deepCopyObject(generator);
            threads.add(new GeneratorThread(copiedGenerator, bus, pipelineId, pipeId, props));
        }
    }
}
