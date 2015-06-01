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

package ezbake.frack.submitter;

import backtype.storm.generated.SupervisorSummary;
import backtype.storm.security.auth.SimpleTransportPlugin;
import backtype.storm.utils.NimbusClient;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.common.properties.EzProperties;
import ezbake.frack.submitter.thrift.PipelineNotRunningException;
import ezbake.frack.submitter.thrift.SubmitResult;
import ezbake.frack.submitter.thrift.Submitter;
import ezbake.frack.submitter.util.ClasspathUtil;
import ezbake.frack.submitter.util.JarUtil;
import ezbake.frack.submitter.util.SubmitterConfiguration;
import ezbake.frack.submitter.util.UnzipUtil;

import ezbake.security.client.EzbakeSecurityClient;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This service is used to submit Pipelines to Frack.
 */
public class SubmitterService extends EzBakeBaseThriftService implements Submitter.Iface {
    private static Logger log = LoggerFactory.getLogger(SubmitterService.class);

    @Option(name="-n", aliases="--insecure", usage="Denotes that all messages broadcast onto the message bus by the submitted pipeline will NOT be secured")
    private boolean insecure = false;

    @Override
    public SubmitResult submit(ByteBuffer zip, String pipelineId) throws TException {
        File unzippedPack = null;
        try {
            // Unzip the provided pack
            unzippedPack = UnzipUtil.unzip(new File("/tmp"), zip);

            // Find the jar path
            Optional<String> jarPath = UnzipUtil.getJarPath(unzippedPack);
            if (!jarPath.isPresent()) {
                return new SubmitResult(false, "Could not find jar file. Make sure to place jar file in zip within bin/ directory");
            }
            final File jarFile = new File(jarPath.get());

            // Get the builder class
            Optional<String> builderClass = ClasspathUtil.findClassInJar(jarFile);
            if (!builderClass.isPresent()) {
                log.error("Could not find PipelineBuilder implementation in {}", jarFile.getName());
                return new SubmitResult(false, "Could not find PipelineBuilder implementation in jar: " + jarFile.getName());
            }
            log.debug("Building pipeline with builder class {}", builderClass.get());

            SubmitterConfiguration config = new SubmitterConfiguration()
                    .setIngestSystem("Storm")
                    .setBuilderClass(builderClass.get())
                    .setPipelineId(pipelineId)
                    .setProduction(!insecure);

            Optional<String> confPath = UnzipUtil.getConfDirectory(unzippedPack);
            List<File> newFiles = Lists.newArrayList();
            if (confPath.isPresent()) { 
                config.setUserPropertiesFolder(confPath.get());
                File confDir = new File(confPath.get());
                File stagingDir = new File(unzippedPack, "staging");
                stagingDir.mkdir();
                
                File newPropertiesDir = new File(stagingDir, "frack_properties");
                FileUtils.copyDirectory(confDir, newPropertiesDir);
                newFiles.add(newPropertiesDir);
                
                Optional<String> sslPath = UnzipUtil.getSSLPath(confDir);
                if (sslPath.isPresent()) {
                    File sslDir = new File(sslPath.get());
                    config.setSslDir(sslDir.getAbsoluteFile().getAbsolutePath());
                    File newSSLDir = new File(stagingDir, "ssl");
                    FileUtils.copyDirectory(sslDir, newSSLDir);
                    newFiles.add(newSSLDir);
                } else {
                    log.warn("No SSL directory found for {}, needed for using Thrift services", jarFile.getName());
                }
                
                File keyDir = UnzipUtil.findSubDir(unzippedPack, "keys");
                if (keyDir != null && keyDir.exists()) {
                    File newKeyDir = new File(stagingDir, "keys");
                    FileUtils.copyDirectory(keyDir, newKeyDir);
                    newFiles.add(newKeyDir);
                } else {
                    log.warn("No Keys directory found for {}, needed for broadcasting", jarFile.getName());
                }
            } else {
                log.warn("No configuration directory found for {}", jarFile.getName());
            }

            // Create the repackaged jar
            log.info("Repackaging jar with configuration information");
            File newJar = JarUtil.addFilesToJar(jarFile, newFiles);
            config.setPathToJar(newJar.getAbsolutePath());

            log.debug("Sending information to PipelineSubmitter");
            SubmitResult result = PipelineSubmitter.submit(config);
            log.info("Submission result: {}", result.getMessage());
            return result;
        } catch (IOException e) {
            String message = "Could not unzip provided build pack binary";
            log.error(message, e);
            throw new TException(message, e);
        } catch (InterruptedException e) {
            String message = "Interrupted exception occurred when submitting pipeline";
            log.error(message, e);
            throw new TException(message, e);
        } catch (ClassNotFoundException e) {
            String message = "PipelineBuilder class not found when interrogating jar file";
            log.error(message, e);
            throw new TException(message, e);
        } finally {
            if (unzippedPack != null && unzippedPack.exists()) {
                try {
                    FileUtils.deleteDirectory(unzippedPack);
                } catch (IOException e) {
                    throw new TException("Could not delete unzipped buildpack directory", e);
                }
            }
        }
    }
    
    @Override
    public void shutdown(String pipelineId) throws PipelineNotRunningException, TException {
        try {
            PipelineSubmitter.shutdownPipeline("Storm", pipelineId);
            log.info("{} successfully shut down", pipelineId);
        } catch (IOException e) {
            log.error("An exception occurred when shutting down {}", pipelineId);
            throw new TException("Exception occurred when shutting down pipeline '" + pipelineId +
                    "'. It may not have shut down properly", e);
        }
    }

    private void run() throws TTransportException {
        Submitter.Processor processor = new Submitter.Processor(this);
        TServerTransport serverTransport = new TServerSocket(8500);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("Starting the frack-submitter service");
        server.serve();
    }

    // FOR TESTING PURPOSES ONLY
    public static void main(String[] args) throws TTransportException {
        SubmitterService handler = new SubmitterService();
        CmdLineParser parser = new CmdLineParser(handler);
        try {
            parser.parseArgument(args);
            handler.run();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    @Override
    public TProcessor getThriftProcessor() {
        // Reset logging configuration
        Properties props = new Properties();
        try {
            InputStream configStream = getClass().getResourceAsStream("/frack-log4j.properties");
            props.load(configStream);
            configStream.close();
        } catch (IOException e) {
            System.out.println("Error: Cannot load configuration file ");
        }
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        return new Submitter.Processor(this);
    }

    @Override
    public boolean ping() {
        EzbakeSecurityClient client = new EzbakeSecurityClient(getConfigurationProperties());
        try {
            // Check connection to Storm
            Map<Object, Object> conf = new HashMap<Object, Object>();
            EzProperties props = new EzProperties(getConfigurationProperties(), true);
            conf.put("nimbus.host", props.getProperty("nimbus.host"));
            conf.put("nimbus.thrift.port", props.getInteger("nimbus.thrift.port", 6627));
            conf.put("storm.thrift.transport", SimpleTransportPlugin.class.getCanonicalName());
            NimbusClient nimbus = NimbusClient.getConfiguredClient(conf);
            int nimbusUptime = nimbus.getClient().getClusterInfo().get_nimbus_uptime_secs();
            if (nimbusUptime < 0) {
                log.error("Nimbus is not connected");
                return false;
            }
            log.info("Nimbus is active");
            List<SupervisorSummary> supervisors = nimbus.getClient().getClusterInfo().get_supervisors();
            if (supervisors.size() == 0) {
                log.error("No supervisors are active");
                return false;
            }
            log.info("Supervisors are up");

            if (client.ping()) {
                log.info("Security service is up");
                return true;
            } else {
                log.error("Security service is down, pipelines will not be able to function.");
                return false;
            }
        } catch (Exception e) {
            log.error("Exception thrown attempting to ping", e);
            return false;
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                log.error("Could not close EzbakeSecurityClient", e);
            }
        }
    }
}
