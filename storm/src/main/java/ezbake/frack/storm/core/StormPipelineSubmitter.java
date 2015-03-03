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

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import com.google.common.collect.Sets;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.*;
import ezbake.frack.storm.core.config.StormConfiguration;

import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class StormPipelineSubmitter implements PipelineSubmitter {
    private static Logger log = LoggerFactory.getLogger(StormPipelineSubmitter.class);

    @Override
    public void submit(final Pipeline pipeline) throws IOException {
        long startTime = System.currentTimeMillis();
        String id = pipeline.getId();
        log.info("Submitting {} to Storm", id);
        NimbusClient client = null;

        try {
            TopologyBuilder builder = new TopologyBuilder();
            StormConfiguration stormConfig = new StormConfiguration(pipeline.getProperties());
            boolean quarantineEnabled = new EzProperties(stormConfig, true).getBoolean(PipelineConfiguration.ENABLE_QUARANTINE, false);

            // I would like to use a HashMultimap here, but the resulting set object is not serializable.
            // Unfortunately this means we have to use our own Multimap with a HashSet object as each value.
            // Each key in this multimap is the pipe ID of stream source, and each value for that key is a
            // stream name on which the source pipe will be emitting Tuples.
            Map<String, Set<String>> streams = new HashMap<>();

            List<Pipeline.PipeInfo> pipeInfos = pipeline.getPipeInfos();

            log.debug("Translating Pipes to Spouts/Bolts");
            Set<String> streamIds = Sets.newHashSet();

            if (quarantineEnabled) {
                QuarantineSpout quarantineSpout = new QuarantineSpout(stormConfig, id, streamIds);
                builder.setSpout(QuarantineSpout.SPOUT_ID, quarantineSpout);
            }

            for (Pipeline.PipeInfo pipeInfo : pipeInfos) {
                String pipeId = pipeInfo.getPipeId();
                Pipe pipe = pipeInfo.getPipe();

                if (!streams.containsKey(pipeId)) {
                    streams.put(pipeId, new HashSet<String>());
                }

                if (Pipeline.isListener(pipe)) {
                    FrackListenerSpout listener = new FrackListenerSpout(pipeInfo, stormConfig, id, streams.get(pipeId), startTime);
                    builder.setSpout(pipeId, listener, stormConfig.getNumExecutors(pipeId)).setNumTasks(stormConfig.getNumTasks(pipeId));
                } else if (Pipeline.isGenerator(pipe)) {
                    log.debug("Adding spout for {}", pipeId);
                    FrackSpout generator = new FrackSpout(pipeInfo, stormConfig, id, streams.get(pipeId), startTime);
                    builder.setSpout(pipeId, generator, stormConfig.getNumExecutors(pipeId)).setNumTasks(stormConfig.getNumTasks(pipeId));
                } else if (Pipeline.isWorker(pipe)) {
                    log.info("Adding bolt for {}", pipeId);

                    FrackBolt frackBolt = new FrackBolt(pipeInfo, stormConfig, id, streams.get(pipeId), startTime);
                    BoltDeclarer declarer = builder.setBolt(pipeId, frackBolt, stormConfig.getNumExecutors(pipeId)).setNumTasks(stormConfig.getNumTasks(pipeId));

                    // Create a stream id which is used by the quarantine spout to emit to specific workers
                    if (quarantineEnabled) {
                        streamIds.add(pipeId);
                        declarer.shuffleGrouping(QuarantineSpout.SPOUT_ID, pipeId);
                    }

                    Worker worker = (Worker) pipe;
                    String streamId = worker.getType().getCanonicalName();
                    for (String input : pipeInfo.getInputs()) {
                        if (!streams.containsKey(input)) {
                            streams.put(input, new HashSet<String>());
                        }
                        log.info("Adding input {} for bolt", input);
                        streams.get(input).add(streamId);
                        log.info("Input {} and Stream {}", input, streamId);
                        declarer.shuffleGrouping(input, streamId);
                    }
                }
            }

            // Upload the jar to the nimbus
            String uploadedLocation = StormSubmitter.submitJar(stormConfig, pipeline.getProperties().getProperty(PipelineConfiguration.JAR_LOCATION_PROP));

            // Start the topology
            client = NimbusClient.getConfiguredClient(stormConfig);
            String serConf = JSONValue.toJSONString(stormConfig);
            client.getClient().submitTopology(pipeline.getId(), uploadedLocation, serConf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            throw new IOException(String.format("Pipeline with name '%s' already exists!", id), e);
        } catch (InvalidTopologyException e) {
            throw new IOException(String.format("Pipeline '%s' is invalid", id), e);
        } catch (IOException e) {
            throw new IOException("Could not initialize default Storm configuration", e);
        } catch (TException e) {
            throw new IOException("Thrift Exception occurred when submitting topology", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
