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

import com.google.common.collect.*;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by eperry on 12/20/13.
 */
public class TestPipelineSubmitter implements PipelineSubmitter {
    private Multimap<String, Worker> workerMap;
    private long timeToRun;
    private int numberOfIterations;

    public TestPipelineSubmitter(long timeout, int numberOfIterations) {
        this.workerMap = HashMultimap.create();
        this.timeToRun = timeout;
        this.numberOfIterations = numberOfIterations;
    }

    @Override
    public void submit(final Pipeline pipeline) {
        Map<String, Generator> generators = Maps.newHashMap();
        SimplePublisher publisher = new SimplePublisher();
        for (Pipeline.PipeInfo info : pipeline.getPipeInfos()) {
            Pipe pipe = info.getPipe();
            pipe.registerPublisher(publisher);

            if (Pipeline.isGenerator(pipe)) {
                generators.put(info.getPipeId(), (Generator) pipe);
            } else if (Pipeline.isWorker(pipe)) {
                for (String input : info.getInputs()) {
                    workerMap.put(input, (Worker)pipe);
                }
            }
        }

        long expiration = System.currentTimeMillis() + timeToRun;
        int iterations = 0;
        try {
            while (System.currentTimeMillis() < expiration && iterations++ < numberOfIterations) {
                for (Map.Entry<String, Generator> entry : generators.entrySet()) {
                    entry.getValue().generate();
                    Multimap<Class<? extends Serializable>, Envelope> data = publisher.getDataBetweenPipes();
                    for (Map.Entry<Class<? extends Serializable>, Envelope> envEntry : data.entries()) {
                        for (Worker worker : workerMap.get(entry.getKey())) {
                            if (worker.getType().equals(envEntry.getKey())) {
                                worker.doWork(envEntry.getValue().getVisibility(), envEntry.getValue().getData());
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
