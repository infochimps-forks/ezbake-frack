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

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.Serializable;

/**
 * Generic base interface for Thrift Converter objects.
 *
 * The most common use for this class is when the workers expect a type that is different from the Thrift object that
 * you are generating. When creating a new pipeline, you will have to create new converters for the following workers:
 *
 * <ul>
 *     <li>{@code InputSerializable} to {@link ezbake.warehaus.Repository} to store in warehaus</li>
 *     <li>{@code InputSerializable} to {@link ezbake.base.thrift.DocumentClassification} to store in warehaus</li>
 *     <li>{@code InputSerializable} to {@code OutputThrift} used to broadcast on topics</li>
 *     <li>{@code InputSerializable} to {@link ezbake.frack.common.utils.thrift.SSRJSON} used to broadcast to SSR topic</li>
 * </ul>
 *
 * {@link PassThruThriftConverter} is a default implementation that exists for feeds that do not require any Thrift type conversions.
 *
 * @param <InputSerializable> The class type of a Thrift-based object to be converted.
 * @param <OutputThrift> The class type of the converted Thrift-based object.
 */
public interface IThriftConverter<InputSerializable extends Serializable, OutputThrift extends TBase> extends Serializable {
    /**
     * Transforms the given Thrift object to another Thrift object.
     * @param inputSerializable The object to be converted.
     * @return The converted object.
     */
    OutputThrift convert(InputSerializable inputSerializable) throws TException;
}
