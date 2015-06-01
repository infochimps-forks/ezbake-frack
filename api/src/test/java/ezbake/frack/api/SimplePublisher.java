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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ezbake.base.thrift.Visibility;
import ezbake.quarantine.thrift.AdditionalMetadata;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by eperry on 2/10/14.
 */
public class SimplePublisher implements Publisher {
    private Multimap<Class<? extends Serializable>, Envelope> dataBetweenPipes;

    public SimplePublisher() {
        dataBetweenPipes = ArrayListMultimap.create();
    }

    @Override
    public void publish(Class<? extends Serializable> clazz, Envelope envelope) {
        dataBetweenPipes.put(clazz, envelope);
    }

    @Override
    public void sendRawToQuarantine(byte[] data, Visibility visibility, String error, AdditionalMetadata additionalMetadata) {}

    public Multimap<Class<? extends Serializable>, Envelope>  getDataBetweenPipes() {
        Multimap<Class<? extends Serializable>, Envelope> result = dataBetweenPipes;
        dataBetweenPipes = ArrayListMultimap.create();
        return result;
    }
}
