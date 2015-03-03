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

package ezbake.frack.common.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import ezbake.frack.api.Pipeline;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This class facilitates interaction with the EzBake INS. It provides static methods that correspond to common
 * operations that Frack pipelines typically need to perform using INS.
 */
public class INSUtil {
    private static Logger logger = LoggerFactory.getLogger(INSUtil.class);

    /**
     * This method returns the INS information for a feed. The INS information includes the Warehaus URI prefix and
     * the set of topics to which the given feed will be broadcasting. NOTE: only non-system topics will be returned
     * from this method. To view system topics, please consult INS.
     *
     * @param pipeline the pipeline object for which we are gathering INS information
     * @param feedName the feed name to gather information for
     * @return the {@link ezbake.frack.common.utils.INSUtil.INSInfo} object containing the URI prefix and broadcast
     *         topics for the given feed
     */
    public static INSInfo getINSInfo(Pipeline pipeline, String feedName) {
        ThriftClientPool pool = null;
        InternalNameService.Client insClient = null;

        Properties props = pipeline.getProperties();
        String applicationSecurityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();

        try {
            pool = new ThriftClientPool(props);
            insClient = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);

            Set<String> topics = insClient.getTopicsForFeed(applicationSecurityId, feedName);
            Set<String> systemTopics = insClient.getSystemTopics();
            String prefix = insClient.getURIPrefix(applicationSecurityId, feedName);

            return new INSInfo(new HashSet<>(Sets.difference(topics, systemTopics)), prefix);
        } catch (TException e) {
            logger.error("Could not retrieve information from INS service", e);
            throw new RuntimeException(e);
        } finally {
            if (pool != null) {
                pool.returnToPool(insClient);
                pool.close();
            }
        }
    }

    public static class INSInfo implements Serializable {
        private String uriPrefix;
        private Set<String> topics;

        public INSInfo(Set<String> topics, String uriPrefix) {
            this.topics = topics;
            this.uriPrefix = uriPrefix;
        }

        public Set<String> getTopics() {
            return topics;
        }

        public String getUriPrefix() {
            return uriPrefix;
        }
    }
}
