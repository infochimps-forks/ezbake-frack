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

package ezbake.frack.submitter.systems;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.security.auth.SimpleTransportPlugin;
import backtype.storm.utils.NimbusClient;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.frack.submitter.thrift.PipelineNotRunningException;
import org.apache.thrift7.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * PipelineKiller for Storm.
 */
public class StormPipelineKiller implements PipelineKiller {

    @Override
    public void kill(String pipelineId) throws IOException, PipelineNotRunningException {
        // Grab a nimbus client using EZConfiguration and kill the topology associated with this pipeline ID
        NimbusClient client = null;
        try {
            EzProperties props = new EzProperties(new EzConfiguration().getProperties(), true);
            Map<Object, Object> conf = new HashMap<>();
            conf.put("nimbus.host", props.getProperty("nimbus.host"));
            conf.put("nimbus.thrift.port", props.getInteger("nimbus.thrift.port", 6627));
            conf.put("storm.thrift.transport", SimpleTransportPlugin.class.getCanonicalName());
            client = NimbusClient.getConfiguredClient(conf);
            KillOptions opts = new KillOptions();
            opts.set_wait_secs(0);
            client.getClient().killTopologyWithOpts(pipelineId, opts);
        } catch (TException e) {
            throw new IOException(String.format("TException occurred during shutdown of '%s'", pipelineId), e);
        } catch (NotAliveException e) {
            throw new PipelineNotRunningException().setMessage(String.format("Pipeline '%s' is not running", pipelineId));
        } catch (EzConfigurationLoaderException e) {
            throw new IOException("Could not load configuration", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
