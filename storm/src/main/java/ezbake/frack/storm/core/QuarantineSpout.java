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
import backtype.storm.tuple.Values;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Envelope;
import ezbake.frack.storm.core.config.StormConfiguration;
import ezbake.quarantine.thrift.ObjectNotQuarantinedException;
import ezbake.quarantine.thrift.ObjectStatus;
import ezbake.quarantine.thrift.QuarantineResult;
import ezbake.quarantine.thrift.QuarantinedObject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class QuarantineSpout implements IRichSpout{
    private static      Logger logger   = LoggerFactory.getLogger(QuarantineSpout.class);
    public static final String SPOUT_ID = "quarantine-generator";
    public static final String SLEEP_TIME_KEY = "quarantine.spout.sleep.time";
    public static final int DEFAULT_SLEEP_TIME = 3*60*1000;

    private int sleepTime;
    private QuarantineProcessor processor;
    private String              pipelineID;

    private ExecutorService    executor;
    private StormConfiguration config;
    private Set<String> streamIds;

    private Semaphore emitLock;

    public QuarantineSpout(StormConfiguration config, String pipelineID, Set<String> streamIds) {
        this.pipelineID = pipelineID;
        this.config = config;
        this.streamIds = streamIds;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        EzProperties ezProps = new EzProperties(config, true);
        sleepTime = ezProps.getInteger(SLEEP_TIME_KEY, DEFAULT_SLEEP_TIME);
        processor = new QuarantineProcessor(ezProps, pipelineID);
        emitLock = new Semaphore(1, true);
        executor = Executors.newFixedThreadPool(1);
        executor.submit(new Runnable() {
            @SuppressWarnings("InfiniteLoopStatement")
            @Override
            public void run() {
                while (true) {
                    try {
                        generate(spoutOutputCollector);
                    } catch (IOException e) {
                        logger.error("Error occurred in data generation. Some data may be lost", e);
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (emitLock.availablePermits() == 0){
            emitLock.release();
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        for(String stream : streamIds){
            outputFieldsDeclarer.declareStream(stream, new Fields(StormConfiguration.ENVELOPE_DATA_FIELD));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void generate(SpoutOutputCollector spoutOutputCollector) throws IOException{
        logger.debug("Entered QuarantineGenerator generate! getting objects now");
        String updateMessage = "Status updated to %s by QuarantineSpout.";
        try {
            // Continue calling the Quarantine service until an ObjectNotQuarantined exception is thrown, which means
            // that all of the objects have been reingested.
            while(true) {
                List<QuarantineResult> resultList = processor.getApprovedObjectsForPipeline(pipelineID, 0, (short)50);

                for (QuarantineResult result : resultList) {
                    QuarantinedObject obj = result.getObject();
                    Envelope envelope =  new Envelope(obj.getVisibility(), obj.getContent());
                    processor.updateStatus(result.getId(), ObjectStatus.PREPARED_TO_REINGEST, String.format(updateMessage, ObjectStatus.PREPARED_TO_REINGEST));
                    spoutOutputCollector.emit(obj.getPipeId(), new Values(envelope), result.getId());
                    processor.updateStatus(result.getId(), ObjectStatus.REINGESTED, String.format(updateMessage, ObjectStatus.REINGESTED));
                }
            }
        } catch (ObjectNotQuarantinedException e) {
            if (!e.getMessage().startsWith("No objects found in quarantine for pipeline ID")) {
                logger.warn("Object Not Quarantined.  Pipeline might not have the right permissions to see the quarantined object", e);
            }
        } catch (TException e) {
            logger.warn("Could not retrieve objects from quarantine", e);
        } finally {
            sleep(sleepTime);
        }
    }

    private void sleep(int ms) {
        try {
            logger.info("Sleeping for " + ms/1000 +" seconds");
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
