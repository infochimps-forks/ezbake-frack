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
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.frack.eventbus.util.CopyHelper;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TBase;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class BroadcastWorkerTest {
    public static final Visibility DEFAULT_VISIBILITY = new Visibility().setFormalVisibility("U");

    @Test
    public void shouldBeSerializableOtherwiseThePipelineWillNotDeploy() throws Exception {
        BroadcastWorker original = new BroadcastWorker<>(StreamEvent.class, new HashSet<String>(), new TestConverter());
        CopyHelper.deepCopyObject(original);
    }

    @SuppressWarnings({"unchecked", "ToArrayCallWithZeroLengthArrayArgument"})
    @Test
    public void shouldRegisterBroadcastTopics() throws Exception {
        DateTime dateTime = TestHelper.buildDateTime();
        StreamEvent event = TestHelper.buildEvent();
        IThriftConverter<StreamEvent, DateTime> broadcaster = when(mock(IThriftConverter.class).convert(event)).thenReturn(dateTime).getMock();
        BroadcastWorker<StreamEvent> worker = new BroadcastWorker<>(StreamEvent.class, buildTopics(), broadcaster);

        assertThat(worker.getBroadcastTopics(), is(buildTopics()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBroadcastTheTransformedThriftObject() throws Exception {
        DateTime dateTime = TestHelper.buildDateTime();
        StreamEvent event = TestHelper.buildEvent();
        IThriftConverter<StreamEvent, DateTime> broadcaster = when(mock(IThriftConverter.class).convert(event)).thenReturn(dateTime).getMock();
        BroadcastWorker<StreamEvent> worker = new BroadcastWorker<>(StreamEvent.class, buildTopics(), broadcaster);
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);

        worker.process(DEFAULT_VISIBILITY, event);

        verify(broadcaster).convert(event);

        for(String topic : buildTopics())
            verify(publisher).broadcast(topic, DEFAULT_VISIBILITY, ThriftUtils.serialize(dateTime));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotHaveToBroadcastIfTopicsEmpty() throws Exception {

        StreamEvent event = TestHelper.buildEvent();
        IThriftConverter<StreamEvent, StreamEvent> broadcaster = mock(IThriftConverter.class);

        BroadcastWorker<StreamEvent> worker = new BroadcastWorker<>(StreamEvent.class, Sets.<String>newHashSet(), broadcaster);
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);
        worker.process(DEFAULT_VISIBILITY, event);

        verify(broadcaster, never()).convert(event);
    }

    private Set<String> buildTopics() {
        return Sets.newHashSet("Topic1", "Topic2");
    }


    private static class TestConverter implements IThriftConverter<StreamEvent, TBase>, Serializable {
        @Override
        public TBase convert(StreamEvent in) {
            return null;
        }
    }
}
