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

import java.io.Serializable;

/**
 * An implementation of an {@link IThriftConverter} in which the {@code convert} method does not make any change to the Thrift object passed in.
 * @param <InThrift> The class type of the input Thrift object.
 */
public class PassThruThriftConverter<InThrift extends TBase> implements Serializable, IThriftConverter<InThrift, InThrift> {
    private static final long serialVersionUID = 1L;
    /**
     * A convert method that simply returns the unmodified input object.
     * @param inThrift a Thrift object
     * @return The same (unmodified) Thrift object
     */
    @Override
    public InThrift convert(InThrift inThrift) {
        return inThrift;
    }
}
