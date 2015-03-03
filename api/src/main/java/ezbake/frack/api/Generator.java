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

import java.io.Serializable;

/**
 * Generator represents a source of streaming data. This class should be extended when a user
 * wishes to provide a new data source. In Pipelines which contain a Generator, the generate()
 * method is called in a tight loop and any data pushed onto the queue (by calling
 * {@link Pipe#outputToPipes} within the generate() method) is distributed to all Pipes
 * connected to the Generator.
 */
public abstract class Generator<T extends Serializable> extends Pipe<T> implements Serializable
{
    /**
     * Generates a new stream of data. This method is called in a tight loop. This method
     * can block if the implementer chooses to do so, but it is also fine if it does not block.
     * See {@link Pipe} for all methods that are available to call from generate().
     */
    public abstract void generate();
}
