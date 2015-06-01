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

import java.io.IOException;

/**
 * This interface must be implemented for every system that implements Frack.
 */
public interface PipelineSubmitter {

    /**
     * This method must take a Pipeline and submit/start the Pipeline.
     *
     * @param pipeline the Pipeline to be submitted
     */
    public void submit(final Pipeline pipeline) throws IOException;
}
