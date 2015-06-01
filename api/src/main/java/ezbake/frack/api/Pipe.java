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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.net.ConnectException;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import ezbake.base.thrift.Visibility;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.quarantine.thrift.AdditionalMetadata;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import ezbake.common.properties.EzProperties;
import ezbake.core.statify.metrics.MetricsReporter;
import ezbake.core.statify.metrics.MetricsProcessor;

/**
 * This class is the base of all classes that can be added to a Pipeline. It defines all common
 * operations between the different components in a Pipeline.
 */
public abstract class Pipe<O extends Serializable> implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Pipe.class);
    private Output<O> output;
    private MetricRegistry registry;
    private MetricsReporter reporter;
    
    public Pipe() {
        this.output = new Output<>();
    }

    /**
     * Initialization method that should be overridden in order to pull configuration
     * values specific to the implementing Pipe. IMPORTANT: All non-serializable (read:
     * a class which implements java.io.Serializable) fields in Pipe subclasses MUST be
     * constructed in this method. If a non-serializable field is constructed before the
     * Pipeline is submitted, the submission will fail.
     *
     * @param props the configuration for the Pipeline
     */
    public void initialize(Properties props) {
        log.debug("Initialize not overridden for this pipe");
    }

    /**
     * This method configures the logging context for the current thread as well as calls
     * the user-defined initialize method.
     *
     * @param props the configuration for the Pipeline
     * @param pipelineId the ID of the Pipeline containing this Pipe
     */
    public final void setup(Properties props, String pipelineId) {
        // Set the pipeline context for the current thread for logging purposes.
        PipelineContext context = new PipelineContext();
        context.setPipelineId(pipelineId);
        PipelineContextProvider.add(context);
        EzProperties properties = new EzProperties(props, false);

        if (properties.getBoolean(PipelineConfiguration.METRICS_MODE, false)) {
            this.registry = new MetricRegistry();
            props.put("statify.identifier", pipelineId);
            int period = properties.getInteger(PipelineConfiguration.METRICS_INTERVAL, 60);
            String unit = properties.getProperty(PipelineConfiguration.METRICS_TIMEUNIT);
            TimeUnit timeUnit = TimeUnit.MINUTES;
            
            if (unit.equalsIgnoreCase("milliseconds")) {
                timeUnit = TimeUnit.MILLISECONDS;
            } else if (unit.equalsIgnoreCase("seconds")) {
                timeUnit = TimeUnit.SECONDS;
            } else if (unit.equalsIgnoreCase("hours")) {
                timeUnit = TimeUnit.HOURS;
            } else {
                timeUnit = TimeUnit.MINUTES;
            }
            
            String processorClass = props.getProperty(PipelineConfiguration.METRICS_PROCESSOR_CLASS);
            if (processorClass == null || processorClass.isEmpty()) {
                log.error("Metrics mode is on, but Metrics Processor Class was not set. To turn on, set [{}] property.", PipelineConfiguration.METRICS_PROCESSOR_CLASS);
                throw new IllegalStateException("Metrics mode is on, but Metrics Processor Class was not set. To set, use the [" + PipelineConfiguration.METRICS_PROCESSOR_CLASS + "] property.");
            }          
            log.info("Initializing processor: " + processorClass);          

            try {      
                MetricsProcessor processor = (MetricsProcessor) Class.forName(processorClass).newInstance();
                processor.initialize(props);
                
                reporter = MetricsReporter.forRegistry(registry)
                        .withProcessor(processor)
                        .withClock(Clock.defaultClock())
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .filter(MetricFilter.ALL)
                        .withName(pipelineId)
                        .build();
                reporter.start(period, timeUnit);
                log.info("Metrics mode is on for pipe.");
            } catch (ConnectException e) {
                throw new RuntimeException("Error connecting to Flume ", e);
            } catch (Exception e) {
                throw new RuntimeException("Error starting metrics ", e);
            }
        } else {
            log.warn("Metrics mode is off. To turn on, set [{}] property to true.", PipelineConfiguration.METRICS_MODE);
        }
        
        // Call the user initialize method
        initialize(props);
    }
    
    public MetricRegistry getRegistry() {
        if (registry == null) {
            throw new IllegalStateException("Metrics registry was not initialized. To turn on, set [" + PipelineConfiguration.METRICS_MODE + "] property to true.");
        }
        return registry;
    }

    /**
     * Cleanup method that should be overridden to properly clean up the pipe when it is
     * taken down.
     */
    public void cleanup() {
    }

    /**
     * This method sends the given Serializable object to any connected Pipes in the Pipeline.
     * This method should be called internally by the {@link Worker#process(Visibility, java.io.Serializable)} or
     * {@link ezbake.frack.api.Generator#generate()} methods in order to propagate the processed data through
     * the Pipeline.
     *
     * @param visibility the Accumulo visibility string representing the classification level of
     *                   the thrift structure being output
     * @param object the Serializable object to send to all connected Pipes
     * @throws IOException
     */
    protected void outputToPipes(Visibility visibility, O object) throws IOException {
        output.toPipes(visibility, object);
    }

    /**
     * This method sends raw data to quarantine.  It should be invoked
     * on failure.
     *
     * This data will not be automatically replayed since there
     * is no way to determine the type of the data.
     *
     * @param data serializable thrift object
     * @param visibility specifies access control for this data
     * @param error a string that should be used to categorize a problem, for example "Could not index data because
     *              the Warehouse service is down". All quarantined objects with the same error string will be
     *              correlated.
     * @param additionalMetadata any additional metadata that would be useful in debugging or characterizing this issue
     * @throws java.io.IOException if the provided data could not be quarantined
     */
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) throws IOException {
        output.rawToQuarantine(data, visibility, error, additionalMetadata);
    }

    /**
     * FOR INTERNAL USE ONLY. CALLING THIS METHOD INSIDE OF A PIPELINE WILL PRODUCE UNEXPECTED BEHAVIOR.
     *
     * @param p publisher to register with this Pipe
     */
    public final void registerPublisher(Publisher p) {
        output.registerPublisher(p);
    }
}
