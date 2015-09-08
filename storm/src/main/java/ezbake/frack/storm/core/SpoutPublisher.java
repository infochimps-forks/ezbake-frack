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

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import ezbake.base.thrift.Visibility;
import ezbake.quarantine.thrift.AdditionalMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import backtype.storm.spout.SpoutOutputCollector;

import ezbake.frack.api.Envelope;
import ezbake.frack.api.Publisher;
import ezbake.frack.api.Pipeline.PipeInfo;

public class SpoutPublisher implements Publisher {

    private PipeInfo pipeInfo;
    private Semaphore emitLock;
    private StormLedger stormLedger;
    private boolean isFaultTolerant = false;
    private SpoutOutputCollector outputCollector;
    private static final Logger log = LoggerFactory.getLogger(SpoutPublisher.class);
    private QuarantineProcessor quarantineProcessor;

    public SpoutPublisher(PipeInfo pipeInfo, SpoutOutputCollector outputCollector, Semaphore emitLock,
                          Properties props, String pipelineId) {
        this.pipeInfo = pipeInfo;
        this.outputCollector = outputCollector;
        this.emitLock = emitLock;
        this.quarantineProcessor = new QuarantineProcessor(props, pipelineId);
    }

    @Override
    public void publish(Class<? extends Serializable> aClass, Envelope envelope) {
	log.trace("publishing {} of class {}.", aClass.getCanonicalName(), envelope);

        if (isFaultTolerant) {
            stormLedger.insert(pipeInfo, envelope);
        }

        try {
            emitLock.acquire();
        } catch (InterruptedException e) {
            // This shouldn't happen
            log.warn("Thread interrupted");
        }

        outputCollector.emit(aClass.getCanonicalName(), new Values(envelope), envelope.getMessageId());
    }

    public void setLedger(StormLedger ledger) {
        /* The insert method is called by publisher.
         * We must check for fault tolerance to ensure ledger is initialized
         * Cleaner to do it in StormLedger, but don't want the unneccessary object overhead */
        stormLedger = ledger;
        isFaultTolerant = true;
    }

    @Override
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        quarantineProcessor.sendRawToQuarantine(pipeInfo.getPipeId(), data, visibility, error, additionalMetadata);
    }
}
