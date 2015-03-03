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

package ezbake.frack.common.workers;

import com.google.common.collect.Sets;
import ezbake.frack.common.utils.thrift.SSRJSON;

import java.io.Serializable;

/**
 * A generic Worker class that sends a Thrift object to the SSR data store.
 *
 * This class requires one {@link ezbake.frack.common.workers.IThriftConverter} collaborator that converts your
 * Thrift object into types required for indexing in SSR:
 * <ul>
 * <li>From {@code T} to {@link ezbake.frack.common.utils.thrift.SSRJSON}</li>
 * </ul>
 *
 * @param <T> class type of the input Thrift object
 */
public class SSRBroadcastWorker<T extends Serializable> extends BroadcastWorker<T> {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the SSR broadcast topic whose value is {@value}.
     */
    public static final String TOPIC_NAME = "SSR";
    /**
     * This is the suffix to use when registering your worker in a pipeline whose value is {@value}.
     * For example:
     *
     *<pre>
     * {@code
     * Pipeline pipeline = new Pipeline();
     * Worker<Article> worker =
     *      new SSRBroadcastWorker<Article>(Article.class,
     *      new SSRBroadcastConverter());
     * String workerId = pipeline.getId() + SSRBroadcastWorker.PIPELINE_SUFFIX;
     * pipeline.addWorker(workerId, worker);
     * }
     * </pre>
     */
    public static final String PIPELINE_SUFFIX = "_ssr_broadcast_worker";

    /**
     * Constructs an instance of SSRBroadcastWorker.
     * @param thriftObjectType The class type of the Thrift object to be converted and stored.
     * @param converter An instance of an {@link IThriftConverter} that will convert the input Thrift object into an SSRJSON object.
     */
    public SSRBroadcastWorker(Class<T> thriftObjectType, IThriftConverter<T, SSRJSON> converter) {
        super(thriftObjectType, Sets.newHashSet(TOPIC_NAME), converter);
    }
}
