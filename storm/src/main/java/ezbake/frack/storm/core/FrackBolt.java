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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.frack.api.Envelope;
import ezbake.frack.api.Worker;
import ezbake.frack.api.Pipeline.PipeInfo;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.storm.core.config.StormConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class FrackBolt implements IRichBolt {
    private static Logger logger = LoggerFactory.getLogger(FrackBolt.class);

    private Worker worker;
    private OutputCollector collector;
    private StormConfiguration config;
    private Collection<String> streams;
    private String pipelineId;
    private StormLedger stormLedger;
    private long startTime;
    private PipeInfo pipeInfo;
    private BoltPublisher publisher;

    public FrackBolt(PipeInfo pipeInfo, StormConfiguration config, String pipelineId, Collection<String> streams, long startTime) {
        this.worker = (Worker) pipeInfo.getPipe();
        this.config = config;
        this.streams = streams;
        this.pipelineId = pipelineId;
        this.pipeInfo = pipeInfo;
        this.startTime = startTime;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        worker.setup(config, pipelineId);
        collector = outputCollector;
        String taskId = String.valueOf(topologyContext.getThisTaskId());

        EzBroadcaster broadcaster = null;
        if (!worker.getBroadcastTopics().isEmpty()) {
            broadcaster = EzBroadcaster.create(config, PipelineContextProvider.get().getPipelineId());

            for (String topic : (Set<String>) worker.getBroadcastTopics()) {
                broadcaster.registerBroadcastTopic(topic);
            }
        }
        publisher = new BoltPublisher(pipeInfo, outputCollector, broadcaster, config, pipelineId);
        worker.registerWorkerPublisher(publisher);

        /* Set up StormLedger if we are running in fault tolerant mode
         * Determine if this task is the primary processor. If so, process items on the Ledger. */
        if (config.isFaultTolerant()) {
            stormLedger = new StormLedger(pipelineId, config);
            publisher.setLedger(stormLedger);

            if (stormLedger.isQueueProcessor(pipeInfo.getPipeId(), topologyContext.getComponentTasks(topologyContext.getComponentId(topologyContext.getThisTaskId())), taskId)) {
                stormLedger.processQueue(pipeInfo, startTime);
                logger.info("{}:{} has processed Ledger", pipeInfo.getPipeId(), taskId);
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Envelope data = (Envelope)tuple.getValueByField(StormConfiguration.ENVELOPE_DATA_FIELD);

        try {
            worker.doWork(data.getVisibility(), data.getData());

            if (config.isFaultTolerant()) {
                stormLedger.delete(pipeInfo, data);
            }
            collector.ack(tuple);
        } catch (IOException e) {
            logger.error("Error processing data, sending to quarantine", e);
        }
    }

    @Override
    public void cleanup() {
        worker.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        for (String stream : streams) {
            outputFieldsDeclarer.declareStream(stream, new Fields(StormConfiguration.ENVELOPE_DATA_FIELD));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // Nobody should ever need to take a look at the config
        return null;
    }
}
