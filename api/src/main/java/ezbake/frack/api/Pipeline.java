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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class defines the flow of execution between Nozzles, Listeners, and Pumps. It has methods available
 * to easily define new connections between Pipes, and define a processing flow.
 */
public class Pipeline
{
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    protected Map<String, PipeInfo> pipes;
    protected Properties props;
    protected String id;

    public static class PipeInfo implements Serializable
    {
        final String pipeId;
        final List<String> inputs;
        final List<String> outputs;
        final Pipe pipe;

        public PipeInfo(String id, Pipe p) {
            this.pipeId = id;
            this.pipe = p;
            this.inputs = Lists.newArrayList();
            this.outputs = Lists.newArrayList();
        }

        public List<String> getInputs()
        {
            return inputs;
        }
        
        public List<String> getOutputs()
        {
            return outputs;
        }

        /**
         * Returns pipe object.
         * @return pipe <code>Pipe</code> the pipe object
         */
        public Pipe getPipe()
        {
            return pipe;
        }

        /**
         * Returns id of this pipe object.
         * @return pipeId <code>String</code> the pipe object
         */
        public String getPipeId()
        {
            return pipeId;
        }

        /**
         * Add an input
         * @param from <code>String</code> the input name
         */
        public void addInput(String from)
        {
            Preconditions.checkNotNull(from);
            inputs.add(from);
        }
        
        /**
         * Add an output
         * @param to <code>String</code> the output name
         */
        public void addOutput(String to)
        {
            Preconditions.checkNotNull(to);
            outputs.add(to);
        }
    }

    public Pipeline()
    {
        log.debug("Instantiating pipeline");
        this.pipes = Maps.newHashMap();

        // The configuration information for this Pipeline is initialized by the Frack Submitter.
        PipelineContext context = PipelineContextProvider.get();
        this.props = context.getProps();
        this.id = context.getPipelineId();
    }

    public String getId() {
        return id;
    }

    /**
     * Adds a Worker to the pipeline.
     * @param id <code>String</code> The id of the pipe
     * @param w <code>Worker</code> The Worker that will be added to the pipeline.
     */
    public void addWorker(String id, Worker w)
    {
        addPipe(id, w);
    }

    /**
     * Adds a Generator to the pipeline.
     * @param id <code>String</code> The id of the pipe
     * @param g <code>Generator</code> The Generator that will be added to the pipeline.
     */
    public void addGenerator(String id, Generator g)
    {
        addPipe(id, g);
    }

    /**
     * Adds a Listener to the pipeline.
     * @param id <code>String</code> The id of the pipe
     * @param l <code>Generator</code> The Listener that will be added to the pipeline.
     */
    public void addListener(String id, Listener l)
    {
        addPipe(id, l);
    }

    /**
     * Adds a connection between two different Pipes based on the IDs given for each Pipe.
     * This method effectively directs the output from one Pipe into another Pipe.
     *
     * @param fromPipe Pipe from which the connection starts
     * @param toPipe Pipe to which the connection will flow
     */
    public void addConnection(String fromPipe, String toPipe)
    {
        // Step 1: check to see if both pipes exist
        Preconditions.checkState(pipes.containsKey(fromPipe), fromPipe + " does not exist!");
        Preconditions.checkState(pipes.containsKey(toPipe), toPipe + " does not exist!");
        log.info("Adding connection from {} to {}", fromPipe, toPipe);

        // Step 2: Add input connection to the pipe info
        pipes.get(toPipe).addInput(fromPipe);
        
        // Step 3: Add output connection to the pipe info
        pipes.get(fromPipe).addOutput(toPipe);
    }

    /**
     * Adds a Pipe to the pipeline.
     * @param id <code>String</code> The id of the pipe
     * @param p <code>Pipe</code> The Pipe object to add to the pipeline.
     */
    void addPipe(String id, Pipe p)
    {
        Preconditions.checkState(!pipes.containsKey(id), id + " is already taken!");
        pipes.put(id, new PipeInfo(id, p));
    }

    /**
     * Retrieves all pipes in the pipeline.
     * @return <code>List<Pipe></code> The list of Pipe objects in the pipeline.
     */
    List<Pipe> getPipes()
    {
        List <Pipe> retVal = Lists.newArrayList();
        for(PipeInfo pi : pipes.values())
        {
            retVal.add(pi.getPipe());
        }
        return retVal;
    }

    /**
     * Returns all defined Pipes as PipeInfo objects.
     *
     * @return List of all Pipes defined in this Pipeline
     */
    public List<PipeInfo> getPipeInfos()
    {
        List <PipeInfo> retVal = Lists.newArrayList();
        for(PipeInfo pi : pipes.values())
        {
            retVal.add(pi);
        }
        return retVal;
    }

    public Properties getProperties() {
        return props;
    }

    /**
     * Determines whether a Pipe is of type Generator.
     * @param p <code>Pipe</code> The Pipe object that needs to be checked.
     * @return isGenerator <code>boolean</code> The boolean result of the check.
     */
    public static boolean isGenerator(Pipe p)
    {
        return p instanceof Generator;
    }

    /**
     * Determines whether a Pipe is of type Worker.
     * @param w <code>Pipe</code> The Pipe object that needs to be checked.
     * @return isWorker <code>boolean</code> The boolean result of the check.
     */
    public static boolean isWorker(Pipe w)
    {
        return w instanceof Worker;
    }

    /**
     * Determines whether a Pipe is of type Listener.
     * @param p <code>Pipe</code> The Pipe object that needs to be checked.
     * @return isListener <code>boolean</code> The boolean result of the check.
     */
    public static boolean isListener(Pipe p) {
        return p instanceof Listener;
    }
}
