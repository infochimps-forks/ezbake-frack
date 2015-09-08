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

package ezbake.frack.storm.core.config;

import backtype.storm.Config;
import backtype.storm.security.auth.SimpleTransportPlugin;
import ezbake.frack.api.PipelineConfiguration;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormConfiguration extends Properties implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(StormConfiguration.class);

    public static final String ENVELOPE_DATA_FIELD = "ezbake.frack.storm.fields.envelope";
    public static final int QUEUE_CHECK_WAIT_TIME_MS = 15000;

    public StormConfiguration(Properties props) throws IOException {
        putAll(props);

	log.info("properties for storm configuration: {}", props);

        put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(getProperty(Config.NIMBUS_THRIFT_PORT)));
        put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, SimpleTransportPlugin.class.getCanonicalName());
        if (getProperty(PipelineConfiguration.MAX_GENERATOR_QUEUE_SIZE) != null) {
            put(Config.TOPOLOGY_MAX_SPOUT_PENDING, Integer.parseInt(getProperty(PipelineConfiguration.MAX_GENERATOR_QUEUE_SIZE)));
        }
        if (getProperty(PipelineConfiguration.MESSAGE_TIMEOUT_SECS) != null) {
            put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, Integer.parseInt(getProperty(PipelineConfiguration.MESSAGE_TIMEOUT_SECS)));
        }

        if (containsKey(PipelineConfiguration.NUM_WORKERS_PROP)) {
            put(Config.TOPOLOGY_WORKERS, Integer.parseInt(getProperty(PipelineConfiguration.NUM_WORKERS_PROP)));
        } else {
            put(Config.TOPOLOGY_WORKERS, 1);
        }

	boolean debug = Boolean.valueOf(getProperty(PipelineConfiguration.DEBUG, "false"));
	log.info("debug topology: {}", debug);
	put(Config.TOPOLOGY_DEBUG, debug);
	
    }

    public int getNumExecutors(String pipeId) {
        String executors = getProperty(PipelineConfiguration.NUM_EXECUTORS_PROP_PREFIX + pipeId);
        if (executors == null) {
            // By default, return the amount of tasks set for this pipe. This will run one thread per task
            return getNumTasks(pipeId);
        } else {
            return Integer.parseInt(executors);
        }
    }

    public int getNumTasks(String pipeId) {
        String tasks = getProperty(PipelineConfiguration.NUM_TASKS_PROP_PREFIX + pipeId);
        if (tasks == null) {
            return 1;
        } else {
            return Integer.parseInt(tasks);
        }
    }

    public boolean isFaultTolerant() {
        return Boolean.parseBoolean(this.getProperty(PipelineConfiguration.FAULT_TOLERANT_MODE, "false"));
    }
}
