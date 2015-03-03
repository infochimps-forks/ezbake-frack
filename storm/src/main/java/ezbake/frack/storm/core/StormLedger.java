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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import ezbake.base.thrift.Visibility;
import ezbake.frack.storm.core.config.StormConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.frack.api.Worker;
import ezbake.frack.api.Envelope;
import ezbake.frack.api.Pipeline.PipeInfo;
import ezbake.frack.accumulo.ledger.AccumuloLedger;

/**
 * Class used for interacting with the Ledger.
 * Currently uses AccumuloLedger. <br />
 * Runs when [ezbake.frack.fault.tolerant.mode] is true.
 */
public class StormLedger implements Serializable { 

	private AccumuloLedger ledger;
	private static final long serialVersionUID = 1L;
	private static Logger log = LoggerFactory.getLogger(StormLedger.class);

	public StormLedger(String pipelineId, Properties props) {
		this.ledger = new AccumuloLedger(pipelineId, props);
	}

	/**
	 * Determines which task should process items from the Ledger. <br />
	 * If there is only one task, then process. Otherwise, lowest taskId gets to process.
	 * 
	 * @param pipeId  The current pipe's id
	 * @param taskIds The list of task Ids for current pipe
	 * @param taskId  The current task's id
	 */
	public boolean isQueueProcessor(String pipeId, List<Integer> taskIds, String taskId) { 
		// If there is only one task, then it should process queue
		if (taskIds.size() == 1) { 
			return true;
		}

		// We'll select the first task Id as lowest. They come ordered anyway...
		int lowestId = taskIds.get(0);

		// But just to be sure
		for (Integer id : taskIds)
		{
			if (lowestId > id) {
				lowestId = id;
			}
		}

		// If the lowest ID matches the current taskId, then we have a winner
		if (lowestId == Integer.parseInt(taskId)) {
			log.info("{} with component task ids {}. Ledger processing task id is {}.", pipeId, taskIds.toString(), lowestId);
			return true;
		}

		return false;
	}

	/**
	 * Determines if ledger has items to process. <br />
	 * 
	 * @param pipelineId  The pipeline's id
	 * @param startTime   The pipeline's start time
	 */
	public boolean hasQueuedItems(String pipelineId, long startTime) {
		return ledger.hasQueuedItems(pipelineId, startTime);
	}

	/**
	 * Inserts a message in the ledger. <br />
	 * 
	 * @param pipeInfo  The current pipe's PipeInfo
	 * @param envelope  The message envelope
	 */
	public void insert(PipeInfo pipeInfo, Envelope envelope) {  	
		try { 
			envelope.setFromPipe(pipeInfo.getPipeId());

			for (String outputId: pipeInfo.getOutputs()) {
				ledger.insert(envelope.getMessageId(), envelope.getFromPipe(), outputId, envelope.getVisibility(), envelope.getData());
			}
		} catch (IOException e) {
			log.error("Failure saving to ledger", e);
		}
	}

	/**
	 * Deletes a message from the ledger. <br />
	 * 
	 * @param pipeInfo  The current pipe's PipeInfo
	 * @param envelope  The message envelope
	 */
	public void delete(PipeInfo pipeInfo, Envelope envelope) { 
		/* Adding messageId to error log to record messages that were processed, but not deleted 
		 * No point throwing the error, if we don't handle or stop execution */
		String messageId = "";
		try {
			messageId = envelope.getMessageId();
			ledger.delete(messageId, envelope.getFromPipe(), pipeInfo.getPipeId(), envelope.getVisibility());
			log.debug("Processed and deleted {} for {}_{}.", envelope.getMessageId(), envelope.getFromPipe(), pipeInfo.getPipeId());
		} catch (IOException e) { 
			log.error("Error deleting data from ledger: " + messageId, e);
		}
	}

	/**
	 * Processes items on the ledger for the specified pipe. <br />
	 * Processes items on the ledger that were present prior to the Pipeline start time. <br />
	 * 
	 * @param pipeInfo  The current pipe's PipeInfo
	 * @param startTime The start time of the <b>pipeline<b>
	 */
	public void processQueue(PipeInfo pipeInfo, long startTime) { 
		try { 
			String output = pipeInfo.getPipeId();
			Worker worker = (Worker) pipeInfo.getPipe();
			log.debug("{} inputs for {}.", pipeInfo.getInputs().size(), output);

			for (String input : pipeInfo.getInputs()) {
                Map<String, Visibility> queue = ledger.
                        getQueuedItems(input, output, startTime);
				log.info("{} processing {} messages from {}.", output, queue.size(), input);

				for (Entry<String, Visibility> entry: queue.entrySet()) {
					byte[] bytes = ledger.get(entry.getKey(), input, output, entry.getValue());
					log.debug("RowId {} has {} bytes.", entry.getKey(), bytes.length);

					worker.doWork(entry.getValue(), bytes);
					ledger.delete(entry.getKey(), input, output, entry.getValue());
					log.debug("Processed and deleted {} for {}_{}.", entry.getKey(), input, output);
					queue.remove(entry.getKey());
				}
			}
		} catch (IOException e) {
			log.error("Error: ", e);
			/* We can stop execution since pipeline hasn't started 
			 * and there is an opportunity to correct situation. */
			throw new RuntimeException(e);
		} 
	}

    /**
     * Blocks the caller until there is nothing left on the ledger to process for the given pipeline ID.
     *
     * @param pipelineId the ID to check in the ledger
     * @param startTime the time at which the pipeline started up
     */
    public void waitForQueue(String pipelineId, long startTime) {
        while (hasQueuedItems(pipelineId, startTime)) {
            try {
                Thread.sleep(StormConfiguration.QUEUE_CHECK_WAIT_TIME_MS);
            } catch (InterruptedException e) {
                log.error("Error: ", e);
            }
        }
    }
}
