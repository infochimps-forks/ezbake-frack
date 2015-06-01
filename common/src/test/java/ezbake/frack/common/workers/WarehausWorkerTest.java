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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.WorkerPublisher;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;
import ezbake.warehaus.Repository;
import ezbake.warehaus.WarehausService;
import ezbake.warehaus.WarehausServiceConstants;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class WarehausWorkerTest {
    final String APP_ID = "myAppId";
    final String SECURITY_ID = "mySecurityId";

    @Test
    @SuppressWarnings("unchecked")
    public void testOutputToPipesShouldGetCalledWithTheArticle() throws Exception {
        final List<ImmutablePair<Visibility, StreamEvent>> actual = newArrayList();
        final StreamEvent event = TestHelper.buildEvent();

        final Visibility tupleVisibility = new Visibility().setFormalVisibility("U");
        final Visibility documentVisibility = new Visibility().setFormalVisibility("TS");
        IThriftConverter<StreamEvent, Visibility> classificationConverter = when(mock(IThriftConverter.class).convert(event)).thenReturn(documentVisibility).getMock();

        final Repository repository = TestHelper.buildRepository();
        IThriftConverter<StreamEvent, Repository> repositoryConverter = when(mock(IThriftConverter.class).convert(event)).thenReturn(repository).getMock();

        final WarehausService.Client mockWarehaus = mock(WarehausService.Client.class);
        final ThriftClientPool mockPool = when(mock(ThriftClientPool.class).getClient("warehaus", WarehausService.Client.class)).thenReturn(mockWarehaus).getMock();
        final EzSecurityTokenWrapper token = getEzSecurityToken();
        final EzbakeSecurityClient mockClient = when(mock(EzbakeSecurityClient.class).fetchAppToken(SECURITY_ID)).thenReturn(token).getMock();


        WarehausWorker<StreamEvent> warehausWorker = new WarehausWorker<StreamEvent>(StreamEvent.class, repositoryConverter, classificationConverter) {
            @Override
            public void initialize(Properties props) {
                this.warehausSecurityId = SECURITY_ID;
                this.pool = mockPool;
                this.securityClient = mockClient;
            }

            @Override
            protected void outputToPipes(Visibility visibility, Serializable thriftStruct) throws IOException {
                StreamEvent outputEvent = (StreamEvent) thriftStruct;
                actual.add(new ImmutablePair<>(visibility, outputEvent));
            }
        };

        warehausWorker.initialize(new Properties());
        warehausWorker.process(tupleVisibility, event);

        assertTrue("Document was not output to pipe", actual.contains(new ImmutablePair<>(documentVisibility, event)));
        verify(mockWarehaus).insert(repository, documentVisibility, token);
        verify(mockPool).getClient("warehaus", WarehausService.Client.class);
        verify(mockPool).returnToPool(mockWarehaus);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSendToQuarantineWhenThrowingException() throws Exception {
        TException e = new TException("test exception");

        final ThriftClientPool mockPool = when(mock(ThriftClientPool.class).getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class)).thenThrow(e).getMock();

        WarehausWorker<StreamEvent> warehausWorker = new WarehausWorker<StreamEvent>(StreamEvent.class, mock(IThriftConverter.class), mock(IThriftConverter.class)) {
            @Override
            public void initialize(Properties props) {
                this.pool = mockPool;
            }
        };
        warehausWorker.initialize(new Properties());
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        warehausWorker.registerWorkerPublisher(publisher);
        final StreamEvent event = TestHelper.buildEvent();
        final Visibility visibility = new Visibility().setFormalVisibility("U");
        warehausWorker.process(visibility, event);

        AdditionalMetadata metadata = new AdditionalMetadata();
        metadata.putToEntries("stacktrace", new MetadataEntry().setValue(StackTraceUtil.getStackTrace(e)));
        verify(publisher).sendObjectToQuarantine(event, visibility, "test exception", metadata);
    }

    private EzSecurityTokenWrapper getEzSecurityToken() {
        return new EzSecurityTokenWrapper(ThriftTestUtils.generateTestSecurityToken("U"));
    }
}
