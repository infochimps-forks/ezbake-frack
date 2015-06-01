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

import backtype.storm.tuple.Tuple;
import ezbake.base.thrift.Visibility;
import ezbake.ezbroadcast.core.EzBroadcaster;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import ezbake.quarantine.thrift.AdditionalMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;

import ezbake.frack.api.Envelope;
import ezbake.frack.api.Pipeline.PipeInfo;
import ezbake.frack.api.WorkerPublisher;

public class BoltPublisher implements WorkerPublisher<Serializable> {

    private PipeInfo pipeInfo;
    private StormLedger stormLedger;
    private EzBroadcaster broadcaster;
    private OutputCollector outputCollector;
    private boolean isFaultTolerant = false;
    private static final Logger log = LoggerFactory.getLogger(BoltPublisher.class);
    private QuarantineProcessor quarantineProcessor;

    public BoltPublisher(PipeInfo pipeInfo, OutputCollector outputCollector, EzBroadcaster broadcaster,
                         Properties props, String pipelineId) {
        this.pipeInfo = pipeInfo;
        this.outputCollector = outputCollector;
        this.broadcaster = broadcaster;
        this.quarantineProcessor = new QuarantineProcessor(props, pipelineId);
    }

    @Override
    public void publish(Class<? extends Serializable> aClass, Envelope envelope) {
        if (isFaultTolerant) {
            stormLedger.insert(pipeInfo, envelope);
        }

        outputCollector.emit(aClass.getCanonicalName(), new Values(envelope));
    }

    @Override
    public void broadcast(String topic, Visibility visibility, byte[] message) throws IOException {
        if (broadcaster != null) {
            broadcaster.broadcast(topic, visibility, message);
        } else {
            log.error("No topics were registered for this worker!");
            throw new RuntimeException("Must register topics with worker before broadcasting to them");
        }
    }

    public void setLedger(StormLedger ledger) {
        /* The insert method is called by publisher.
         * We must check for fault tolerance to ensure ledger is initialized
         * Cleaner to do it in StormLedger, but don't want the unneccessary object overhead */
        stormLedger = ledger;
        isFaultTolerant = true;
    }

    @Override
    public void sendObjectToQuarantine(Serializable object, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        quarantineProcessor.sendObjectToQuarantine(pipeInfo.getPipeId(), object, visibility, error, additionalMetadata);
    }

    @Override
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        quarantineProcessor.sendRawToQuarantine(pipeInfo.getPipeId(), data, visibility, error, additionalMetadata);
    }
}
