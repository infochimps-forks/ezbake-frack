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

import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import ezbake.ezbroadcast.core.EzBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.topology.IRichSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import ezbake.frack.api.Listener;
import ezbake.frack.api.Pipeline.PipeInfo;
import ezbake.frack.storm.core.config.StormConfiguration;

public class FrackListenerSpout implements IRichSpout {
    private static Logger log = LoggerFactory.getLogger(FrackListenerSpout.class);

    private String pipelineId;

    private PipeInfo pipeInfo;
    private Listener listener;
    private Semaphore emitLock;
    private StormConfiguration config;
    private EzBroadcaster broadcaster;
    private Collection<String> streams;
    private long startTime;

    public FrackListenerSpout(PipeInfo pipeInfo, StormConfiguration config, String pipelineId, Collection<String> streams, long startTime) {
        this.config = config;
        this.streams = streams;
        this.pipeInfo = pipeInfo;
        this.pipelineId = pipelineId;
        this.listener = (Listener) pipeInfo.getPipe();
        this.startTime = startTime;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        broadcaster = EzBroadcaster.create(config, pipelineId);
        for (String topic : (Set<String>) listener.getListenerTopics()) {
            broadcaster.subscribeToTopic(topic);
        }
        /* Set up StormLedger if we are running in fault tolerant mode
         * Pause processing of new data until all items on Ledger are completed. */
        log.info("Running in Fault Tolerant Mode: " + config.isFaultTolerant());
        emitLock = new Semaphore(1, true);
        SpoutPublisher publisher = new SpoutPublisher(pipeInfo, spoutOutputCollector, emitLock, config, pipelineId);
        if (config.isFaultTolerant()) {
            StormLedger stormLedger = new StormLedger(pipelineId, config);
            publisher.setLedger(stormLedger);

            stormLedger.waitForQueue(pipelineId, startTime);
            log.info("Ledger is empty - starting FrackSpout.");
        }

        listener.setup(config, pipelineId);
        broadcaster.startListening(new StormReceiver(publisher, listener.getType()));
    }

    @Override
    public void close() {
        listener.cleanup();
        try {
            broadcaster.close();
        } catch (IOException e) {
            log.error("Could not properly shut down broadcaster", e);
        }
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
