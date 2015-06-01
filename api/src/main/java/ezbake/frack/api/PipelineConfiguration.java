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

/**
 * This class contains all of the constants associated with Frack Pipelines. These constants can
 * be used in the ezconfiguration files for a particular pipeline in order to change things like
 * the parallelism for a pipeline, and the number of messages allowed onto the queue for a pipeline.
 */
public class PipelineConfiguration {
    /**
     * FOR INTERNAL USE ONLY. This property denotes the location of the pipeline jar.
     * This should not be set by pipeline implementers.
     */
    public static final String JAR_LOCATION_PROP = "ezbake.frack.pipeline.jar";

    /**
     * Use this property to set the number of workers for the pipeline
     */
    public static final String NUM_WORKERS_PROP = "ezbake.frack.api.num.workers";

    /**
     * Use this property prefix (with the pipe name appended to the end) to set the number
     * of executors for the given pipe
     */
    public static final String NUM_EXECUTORS_PROP_PREFIX = "ezbake.frack.api.num.executors.";

    /**
     * Use this property prefix (with the pipe name appended to the end) to set the number
     * of tasks for the given pipe
     */
    public static final String NUM_TASKS_PROP_PREFIX = "ezbake.frack.api.num.tasks.";

    /**
     * Use this property to set the maximum amount of items that can be queued by a Generator
     * before the ingest system blocks calls to outputToPipes. This will allow Worker queues
     * to be worked off rather than flooding the queues.
     */
    public static final String MAX_GENERATOR_QUEUE_SIZE = "ezbake.frack.api.queue.size";

    /**
     * Use this property to set the message timeout for each piece of data that gets emitted through
     * the pipeline. If the message doesn't get processed through the pipeline within the allotted time
     * it will appear as a failure. It will also be cleared off of any queues, so messages will begin
     * flowing through the pipeline as if the message had been processed.
     */
    public static final String MESSAGE_TIMEOUT_SECS = "ezbake.frack.api.message.timeout.secs";

    /**
     * Use this property to set the pipeline to fault tolerant mode. In fault tolerant mode
     * every message that is emitted through the pipeline will be stored to a ledger in case of a catastrophic
     * failure such as power loss or hardware failure. This way, when the pipeline is brought back up,
     * all messages that were in flight will continue to be processed. This will have a performance impact
     * on the pipeline.
     */
    public static final String FAULT_TOLERANT_MODE = "ezbake.frack.api.fault.tolerant.mode";

    /**
     * Use this property to set the pipeline to run in metrics mode and specify the following properties:
     * <ul><li>ezbake.frack.api.metrics.processor.class</li><li>ezbake.frack.api.metrics.mode.interval</li><li>ezbake.frack.api.metrics.mode.timeunit</li></ul>
     * In metrics mode, every message that is emitted through the pipeline will be measured by the metrics defined in pipe.
     * This will have a performance impact on the pipeline.   
     */
    public static final String METRICS_MODE = "ezbake.frack.api.metrics.mode";
    
    /**
     * Use this property to set the pipeline metrics reporting interval. The reporter will poll at the specified interval.
     * Combine the interval with the timeunit property. 
     */
    public static final String METRICS_INTERVAL = "ezbake.frack.api.metrics.mode.interval";
    
    /**
     * Use this property to set the pipeline time unit for the metrics reporting interval.
     * <ul><li>MILLISECONDS</li><li>SECONDS</li><li>MINUTES</li><li>HOURS</li></ul>
     * Defaults to TimeUnit.MINUTES
     */
    public static final String METRICS_TIMEUNIT = "ezbake.frack.api.metrics.mode.timeunit";
    
    /**
     * If the pipeline is in "metrics.mode", use this property to specify the pipeline's metrics processor.
     * Provide the fully qualified name. 
     */
    public static final String METRICS_PROCESSOR_CLASS = "ezbake.frack.api.metrics.processor.class";

    /**
     * When this configuration value is set to true, Frack will inject a Quarantine generator into the pipeline
     * being deployed. The Quarantine generator will periodically retrieve Quarantined items and reingest them by
     * sending them back to the {@link ezbake.frack.api.Worker#process(ezbake.base.thrift.Visibility, java.io.Serializable)} method of the
     * Worker from which the object was Quarantined.
     */
    public static final String ENABLE_QUARANTINE = "ezbake.frack.api.quarantine.enable";
}
