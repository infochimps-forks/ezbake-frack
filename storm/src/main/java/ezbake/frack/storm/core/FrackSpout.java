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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ezbake.frack.api.Generator;
import ezbake.frack.api.Pipeline.PipeInfo;
import ezbake.frack.storm.core.config.StormConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class FrackSpout implements IRichSpout {
    private static Logger log = LoggerFactory.getLogger(FrackSpout.class);

    private long startTime;
    private String pipelineId;

    private PipeInfo pipeInfo;
    private Semaphore emitLock;
    private Generator generator;
    private ExecutorService executor;
    private StormConfiguration config;
    private Collection<String> streams;

    public FrackSpout(PipeInfo pipeInfo, StormConfiguration config, String pipelineId, Collection<String> streams, long startTime) {
        this.config = config;
        this.streams = streams;
        this.pipeInfo = pipeInfo;
        this.startTime = startTime;
        this.pipelineId = pipelineId;
        this.generator = (Generator) pipeInfo.getPipe();
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        generator.setup(config, pipelineId);
        emitLock = new Semaphore(1, true);
        SpoutPublisher publisher = new SpoutPublisher(pipeInfo, spoutOutputCollector, emitLock, config, pipelineId);

        /* Set up StormLedger if we are running in fault tolerant mode
         * Pause processing of new data until all items on Ledger are completed. */
        log.info("Running in Fault Tolerant Mode: " + config.isFaultTolerant());
        if (config.isFaultTolerant()) {
            StormLedger stormLedger = new StormLedger(pipelineId, config);
            publisher.setLedger(stormLedger);

            stormLedger.waitForQueue(pipelineId, startTime);
            log.info("Ledger is empty - starting FrackSpout.");
        }

        this.generator.registerPublisher(publisher);
        this.executor = Executors.newFixedThreadPool(1);
        this.executor.submit(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    generator.generate();
                }
            }
        });
    }

    @Override
    public void close() {
        generator.cleanup();
        executor.shutdownNow();
    }

    @Override
    public void nextTuple() {
        // To ensure that we are abiding by Storm's max spout pending, we need to only emit if
        // nextTuple is called.
        if (emitLock.availablePermits() == 0) {
            emitLock.release();
        }
    }

    @Override
    public void activate() {
        // TODO is there anything we need to do here?
    }

    @Override
    public void deactivate() {
        // TODO is there anything we need to do here?
    }

    @Override
    public void ack(Object o) {
        // TODO Reassess in drop 2
    }

    @Override
    public void fail(Object o) {
        // TODO Reassess in drop 2
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        for (String stream : streams) {
            outputFieldsDeclarer.declareStream(stream, new Fields(StormConfiguration.ENVELOPE_DATA_FIELD));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // Nobody should ever need to take a look at the configuration
        return null;
    }
}
