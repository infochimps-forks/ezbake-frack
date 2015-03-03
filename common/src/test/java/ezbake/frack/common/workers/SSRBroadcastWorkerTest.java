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
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.WorkerPublisher;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.frack.eventbus.util.CopyHelper;
import ezbake.thrift.ThriftUtils;
import org.junit.Test;

import java.io.Serializable;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
public class SSRBroadcastWorkerTest {

    public static final Visibility DEFAULT_VISIBILITY = new Visibility().setFormalVisibility("U");

    @Test
    public void shouldBeSerializableOtherwiseThePipelineWillNotDeploy() throws Exception {
        BroadcastWorker original = new SSRBroadcastWorker<>(StreamEvent.class, new TestConverter());
        CopyHelper.deepCopyObject(original);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldRegisterBroadcastTopics() throws Exception {
        StreamEvent event = TestHelper.buildEvent();
        IThriftConverter<StreamEvent, SSRJSON> ssrBroadcaster = when(mock(IThriftConverter.class).convert(event)).thenReturn(event).getMock();
        SSRBroadcastWorker<StreamEvent> worker = new SSRBroadcastWorker<>(StreamEvent.class, ssrBroadcaster);

        assertThat(worker.getBroadcastTopics(), is((Set<String>) Sets.newHashSet("SSR")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBroadcastTheTransformedThriftObject() throws Exception {
        DateTime dateTime = TestHelper.buildDateTime();
        StreamEvent event = TestHelper.buildEvent();
        IThriftConverter<StreamEvent, SSRJSON> ssrBroadcaster = when(mock(IThriftConverter.class).convert(event)).thenReturn(dateTime).getMock();

        SSRBroadcastWorker<StreamEvent> worker = new SSRBroadcastWorker<>(StreamEvent.class, ssrBroadcaster);
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);

        worker.process(DEFAULT_VISIBILITY, event);
        verify(publisher).broadcast("SSR", DEFAULT_VISIBILITY, ThriftUtils.serialize(dateTime));
    }


    private static class TestConverter implements IThriftConverter<StreamEvent, SSRJSON>, Serializable {
        @Override
        public SSRJSON convert(StreamEvent in) {
            return null;
        }
    }
}
