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

package ezbake.data.ssr.frack.indexing;

import ezbake.frack.api.Listener;
import ezbake.frack.api.Pipeline;
import ezbake.frack.api.PipelineBuilder;
import ezbake.frack.common.utils.thrift.SSRJSON;

public class SSRIndexingBuilder implements PipelineBuilder {

    @Override
    public Pipeline build() {
        Pipeline indexingPipeline = new Pipeline();
        Listener ssrListener = new Listener<>(SSRJSON.class);
        ssrListener.registerListenerTopic("SSR");

        indexingPipeline.addListener("ssr-listener", ssrListener);
        indexingPipeline.addWorker("ssr-indexer", new SSRWorker(SSRJSON.class));
        indexingPipeline.addWorker("ssr-json", new SSRJSONWorker(SSRJSON.class));
        indexingPipeline.addConnection("ssr-listener", "ssr-indexer");
        indexingPipeline.addConnection("ssr-listener", "ssr-json");

        return indexingPipeline;
    }
}
