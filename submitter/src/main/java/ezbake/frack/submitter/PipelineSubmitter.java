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

import com.google.common.base.Optional;
import ezbake.common.properties.DuplicatePropertyException;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.submitter.systems.PipelineKiller;
import ezbake.frack.submitter.thrift.PipelineNotRunningException;
import ezbake.frack.submitter.thrift.SubmitResult;
import ezbake.frack.submitter.util.ClasspathUtil;
import ezbake.frack.submitter.util.SubmitterClass;
import ezbake.frack.submitter.util.SubmitterConfiguration;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * This class handles all of the preparation and submission of a frack pipeline.
 */
public class PipelineSubmitter {
    private static Logger log = LoggerFactory.getLogger(PipelineSubmitter.class);

    @Option(name="-p", aliases="--pathToJar", usage="The path to the jar file containing the Pipeline builder. Path may be relative.", required=true)
    private String pathToJar;

    @Option(name="-i", aliases="--pipelineId", usage="The ID of the Pipeline being submitted", required=true)
    private String pipelineId;

    @Option(name="-n", aliases="--insecure", usage="Denotes that all messages broadcast onto the message bus by the submitted pipeline will NOT be secured")
    private boolean insecure = false;

    @Option(name="-c", aliases="--conf", usage="Overrides the configuration directory for this Pipeline submission")
    private String configurationDir;

    /**
     * This method sets up the environment for a Frack submission and pushes the given pipeline to the
     * requested underlying ingest system.
     *
     * @param config configuration information describing the pipeline submission
     * @return a SubmitResult describing the result of the pipeline submission
     * @throws IOException
     * @throws InterruptedException
     */
    public static SubmitResult submit(final SubmitterConfiguration config) throws IOException, InterruptedException {
        final String submitterClass = SubmitterClass.getSubmitterClass(config.getIngestSystem());

        // If the script is being used locally, the configuration directory will be overridden
        // to use a configuration folder/file in the current workspace
        if (config.getOverrideConfigDir() != null) {
            File configDir = new File(config.getOverrideConfigDir());
            if (!configDir.isDirectory()) {
                return new SubmitResult(false, "Please create configuration directory at " + configDir.getAbsolutePath() +
                        " and place the provided site configuration file in that directory");
            }
            System.setProperty("EZCONFIGURATION_DIR", configDir.getAbsolutePath());
        }

        // Check to make sure the given jar file exists and retrieve the full path
        final File jarFile = new File(config.getPathToJar());
        if (!jarFile.exists() || !jarFile.isFile()) {
            return new SubmitResult(false, "No jar file found at " + config.getPathToJar());
        }

        // Success message
        final SubmitResult result = new SubmitResult();
        result.setMessage(String.format("Pipeline %s submitted successfully!", config.getPipelineId()));
        result.setSubmitted(true);

        URLClassLoader contextClassLoader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()}, Thread.currentThread().getContextClassLoader());

        // Submit the pipeline in a new thread so that we can have full control over the classloader being used for that thread.
        // This allows us to build the Pipeline with classes resolved from the user's jar.
        Thread submissionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Set the pipeline context for the current thread. We are setting this here because there may be
                    // log entries made outside the scope of the Pipes that we want to capture before the pipeline
                    // is actually submitted. The context is set again in the Pipes themselves to capture those logs.
                    final PipelineContext context = new PipelineContext();
                    context.setPipelineId(config.getPipelineId());
                    PipelineContextProvider.add(context);

                    log.info("Submitting Pipeline to {}", config.getIngestSystem());

                    log.debug("Loading PipelineSubmitter class from classloader");
                    Class<?> submitterClazz = Thread.currentThread().getContextClassLoader().loadClass(submitterClass);
                    log.debug("Loading PipelineBuilder from classloader");
                    Class<?> builderClazz = Thread.currentThread().getContextClassLoader().loadClass(config.getBuilderClass());

                    log.info("Starting submission of '{}'", config.getPipelineId());

                    EzProperties props = new EzProperties(new EzConfiguration().getProperties(), true);
                    // Get the user's properties and merge them into the system properties
                    if (config.getUserPropertiesFolder() != null) {
                        log.info("Loading properties from directory '{}'", config.getUserPropertiesFolder());
                        props.loadFromDirectory(config.getUserPropertiesFolder(), "*.properties", true);
                    }

                    // Set the SSL directory
                    if (config.getSslDir() != null) {
                        props.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, config.getSslDir());
                    }

                    log.debug("About to merge properties for {}", config.getPipelineId());
                    props.setProperty("broadcaster.security.production", Boolean.toString(config.isProduction()));
                    props.setProperty("ezbake.frack.pipeline.jar", jarFile.getAbsoluteFile().getAbsolutePath());
                    context.setProps(props);

                    // When we build and submit the Pipeline, we need to make sure that the PipelineBuilder and PipelineSubmitter
                    // are both loaded from the jar the user submitted. So, we use reflection here to get an instance of
                    // the classes we need, and then invoke the methods from the classes.
                    Method build = builderClazz.getDeclaredMethod("build");
                    build.setAccessible(true);
                    Object pipeline = build.invoke(builderClazz.newInstance());

                    log.info("Submitting {} to {}", config.getPipelineId(), submitterClazz.getName());
                    Method submit = submitterClazz.getDeclaredMethod("submit", new Class[]{Thread.currentThread().getContextClassLoader().loadClass("ezbake.frack.api.Pipeline")});
                    submit.setAccessible(true);
                    submit.invoke(submitterClazz.newInstance(), pipeline);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    result.setMessage("Could not instantiate " + submitterClass + ". Please verify that 'frack' is a dependency listed in your pipeline pom file. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                } catch (IOException e) {
                    result.setMessage("Submission failed. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                } catch (NoSuchMethodException e) {
                    result.setMessage("Build method not found for PipelineBuilder. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                } catch (InvocationTargetException e) {
                    result.setMessage("Pipeline submission failed. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                } catch (EzConfigurationLoaderException e) {
                    result.setMessage("Pipeline submission failed, could not load configuration. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                } catch (DuplicatePropertyException e) {
                    result.setMessage("Pipeline submission failed, application configuration contained a duplicate. Stacktrace: \n" + getStackTrace(e));
                    result.setSubmitted(false);
                }
            }
        });
        submissionThread.setContextClassLoader(contextClassLoader);
        submissionThread.start();
        submissionThread.join();

        return result;
    }

    private static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Shut down a currently running pipeline.
     *
     * @param ingestSystem the underlying ingest system that the pipeline is running on
     * @param pipelineId ID of pipeline to shut down
     * @throws IOException
     */
    public static void shutdownPipeline(String ingestSystem, String pipelineId) throws IOException, PipelineNotRunningException {
        String killerClass = SubmitterClass.getKillerClass(ingestSystem);
        try {
            PipelineKiller killer = (PipelineKiller) Class.forName(killerClass).newInstance();
            killer.kill(pipelineId);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    protected SubmitResult submit() throws IOException, InterruptedException, ClassNotFoundException {
        SubmitterConfiguration config = new SubmitterConfiguration();
        File jarFile = new File(pathToJar);
        if (!jarFile.exists()) {
            return new SubmitResult(false, "Jar file does not exist at " + jarFile.getAbsolutePath());
        }
        Optional<String> builderClass = ClasspathUtil.findClassInJar(jarFile);
        if (!builderClass.isPresent()) {
            return new SubmitResult(false, "Could not find PipelineBuilder implementation in " + jarFile.getAbsolutePath());
        }

        config.setBuilderClass(builderClass.get())
                .setIngestSystem("EventBus")
                .setPathToJar(jarFile.getAbsolutePath())
                .setProduction(!insecure)
                .setUserPropertiesFolder("user-conf" + File.separator + pipelineId)
                .setPipelineId(pipelineId);

        if (configurationDir != null) {
            config.setOverrideConfigDir(configurationDir);
        }

        return PipelineSubmitter.submit(config);
    }

    /**
     * When running the PipelineSubmitter as an executable, simply enter the required parameters on the
     * command line.
     *
     * @param args command line args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        PipelineSubmitter submitter = new PipelineSubmitter();
        CmdLineParser parser = new CmdLineParser(submitter);
        try {
            parser.parseArgument(args);
            System.out.println(submitter.submit().getMessage());
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }
}
