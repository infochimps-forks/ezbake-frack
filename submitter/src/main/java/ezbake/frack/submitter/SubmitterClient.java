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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.base.Strings;

import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.frack.submitter.thrift.SubmitResult;
import ezbake.frack.submitter.thrift.Submitter;
import ezbake.frack.submitter.thrift.submitterConstants;
import ezbake.thrift.ThriftClientPool;

public class SubmitterClient {

    @Option(name = "-z", aliases = "--pathToTarGz", usage = "Path to the .tar.gz file being uploaded")
    String pathToTarGz;

    @Option(name = "-p", aliases = "--pipelineId", usage = "Pipeline ID being submitted or shutdown")
    String pipelineId;

    @Option(name = "-s", aliases = "--securityId", usage = "The security ID to use when creating an SSL connection to"
            + " the submitter service", required = true)
    String securityId;

    @Option(name = "-u", aliases = "--submit", usage = "Denotes that this will be a submit request")
    boolean submit = false;

    @Option(name = "-d", aliases = "--shutdown", usage = "Denotes that this will be a shutdown request")
    boolean shutdown = false;

    @Option(name = "-i", aliases = "--ping", usage = "Sends a ping request to the submitter service")
    boolean ping = false;

    public static void main(String[] args) throws IOException, InterruptedException, TException {
        SubmitterClient client = new SubmitterClient();
        CmdLineParser parser = new CmdLineParser(client);
        try {
            parser.parseArgument(args);
            client.run(parser);
        } catch (CmdLineException | EzConfigurationLoaderException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    private void run(CmdLineParser parser)
            throws TException, IOException, CmdLineException, EzConfigurationLoaderException {
        if (!(submit || shutdown || ping)) {
            throw new CmdLineException(parser, "Must provide either -u or -d option to client");
        }
        Properties props = new EzConfiguration().getProperties();
        props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, securityId);
        ThriftClientPool pool = new ThriftClientPool(props);
        Submitter.Client client = pool.getClient(submitterConstants.SERVICE_NAME, Submitter.Client.class);
        try {
            if (submit) {
                if (Strings.isNullOrEmpty(pipelineId)) {
                    throw new CmdLineException(parser, "Pipeline ID required for submission");
                }
                File zipFile = new File(pathToTarGz);
                byte[] fileBytes = FileUtils.readFileToByteArray(zipFile);
                SubmitResult result = client.submit(ByteBuffer.wrap(fileBytes), pipelineId);
                System.out.println(result.getMessage());
            } else if (shutdown) {
                if (Strings.isNullOrEmpty(pipelineId)) {
                    throw new CmdLineException(parser, "Pipeline ID required for shutdown");
                }
                client.shutdown(pipelineId);
            } else {
                boolean healthy = client.ping();
                System.out.println(healthy ? "The service is healthy!" : "The service is unhealthy!");
            }
        } finally {
            if (client != null) {
                pool.returnToPool(client);
                pool.close();
            }
        }
    }
}
