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

import com.fasterxml.uuid.Generators;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.WorkerPublisher;
import ezbake.frack.common.utils.thrift.ProvenanceRegistration;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.provenance.thrift.AddDocumentResult;
import ezbake.services.provenance.thrift.AddDocumentStatus;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ProvenanceBatchWorkerTest {
    final String SECURITY_ID = "mySecurityId";

    class UriItem implements Serializable {
        public String uri;

        public UriItem(String uri) {
            this.uri = uri;
        }
    }

    private static final String THE_URI = "this-is-a-uri";
    private ProvenanceBatchWorker<UriItem> worker;
    private ProvenanceService.Client mockProvenanceClient;
    private List<Boolean> outputToPipesCalled;
    private Visibility documentVisibility;

    private ThriftClientPool mockPool;
    private EzbakeSecurityClient mockClient;
    private Map<String, AddDocumentResult> resultMap;

    class UriConverter implements Serializable, IThriftConverter<UriItem, ProvenanceRegistration> {
        @Override
        public ProvenanceRegistration convert(UriItem item) {
            return new ProvenanceRegistration(item.uri);
        }
    }

    @Before
    public void setUp() throws Exception {

        documentVisibility = new ezbake.base.thrift.Visibility().setFormalVisibility("U");
        final EzSecurityTokenWrapper token = getEzSecurityToken();
        mockClient = when(mock(EzbakeSecurityClient.class).fetchAppToken(SECURITY_ID)).thenReturn(token).getMock();
        mockProvenanceClient = mock(ProvenanceService.Client.class);
        mockPool = when(mock(ThriftClientPool.class).getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class)).thenReturn(mockProvenanceClient).getMock();
        resultMap = ImmutableMap.of(THE_URI, new AddDocumentResult(AddDocumentStatus.SUCCESS));
        outputToPipesCalled = Lists.newArrayList();

        when(mockProvenanceClient.addDocuments(any(EzSecurityTokenWrapper.class), any(Set.class), any(Set.class))).thenReturn(resultMap);

        worker = new ProvenanceBatchWorker<UriItem>(UriItem.class, new UriConverter()) {
            @Override
            public void initialize(Properties props) {
                this.outputDuplicates = false;
                this.maxQueueSize = 1;
                this.pool = mockPool;
                this.securityClient = mockClient;
                this.addDocumentEntrySet = Sets.newHashSet();
                this.ageOffMappingSet = Sets.newHashSet();
                this.uriToObjectMap = Maps.newHashMap();
            }

            @Override
            protected void outputToPipes(Visibility visibility, Serializable object) {
                outputToPipesCalled.add(true);
            }
        };
        worker.initialize(new Properties());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallProvenanceAddDocumentsViaFlushQueue() throws Exception {
        worker.process(documentVisibility, new UriItem(THE_URI));
        verify(mockProvenanceClient).addDocuments(any(EzSecurityTokenWrapper.class), any(Set.class), any(Set.class));
        assertThat(outputToPipesCalled.isEmpty(), is(false));
    }

    @Test
    public void shouldProvenaceByQueueSize() throws Exception {
        final int QUEUE_SIZE = 3;
        final int NUM_DOCUMENTS = 6;

        worker = new ProvenanceBatchWorker<UriItem>(UriItem.class, new UriConverter()) {
            @Override
            public void initialize(Properties props) {
                this.outputDuplicates = false;
                this.maxQueueSize = QUEUE_SIZE;
                this.pool = mockPool;
                this.securityClient = mockClient;
                this.addDocumentEntrySet = Sets.newHashSet();
                this.ageOffMappingSet = Sets.newHashSet();
                this.uriToObjectMap = Maps.newHashMap();
            }

            @Override
            protected void outputToPipes(Visibility visibility, Serializable object) {
                outputToPipesCalled.add(true);
            }
        };
        worker.initialize(new Properties());

        resultMap = generateResultMap(NUM_DOCUMENTS,AddDocumentStatus.SUCCESS);
        when(mockProvenanceClient.addDocuments(any(EzSecurityTokenWrapper.class), any(Set.class), any(Set.class))).thenReturn(resultMap);

        for (String uri : resultMap.keySet()) {
            worker.process(documentVisibility, new UriItem(uri));
        }

        verify(mockProvenanceClient, times(2)).addDocuments(any(EzSecurityTokenWrapper.class), any(Set.class), any(Set.class));
        assertThat(outputToPipesCalled.isEmpty(), is(false));
    }

    @Test
    public void shouldCallQuarantineWhenProvenanceReturnsFailures() throws Exception {
        final int numDocuments = 1;
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);

        resultMap = generateResultMap(numDocuments,AddDocumentStatus.UNKNOWN_ERROR);
        when(mockProvenanceClient.addDocuments(any(EzSecurityTokenWrapper.class), any(Set.class), any(Set.class))).thenReturn(resultMap);

        for (String uri : resultMap.keySet()) {
            worker.process(documentVisibility, new UriItem(uri));
        }
        verify(publisher).sendObjectToQuarantine(any(Serializable.class), any(Visibility.class), any(String.class), any(AdditionalMetadata.class));
        assertThat(outputToPipesCalled.isEmpty(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallQuarantineWhenResultMapDoesNotContainURI() throws Exception {
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);
        worker.process(documentVisibility, new UriItem("NOT" + THE_URI));
        verify(publisher).sendObjectToQuarantine(any(Serializable.class), any(Visibility.class), any(String.class), any(AdditionalMetadata.class));
        assertThat(outputToPipesCalled.isEmpty(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallQuarantineWhenConverterThrowsTException() throws Exception {

        class UriConverter implements Serializable, IThriftConverter<UriItem, ProvenanceRegistration> {
            @Override
            public ProvenanceRegistration convert(UriItem item) throws TException {
                throw new TException("catch me!");
            }
        }
        WorkerPublisher publisher = mock(WorkerPublisher.class);
        worker.registerWorkerPublisher(publisher);

        worker.process(documentVisibility, new UriItem("NOT" + THE_URI));

        verify(publisher).sendObjectToQuarantine(any(Serializable.class), any(Visibility.class), any(String.class), any(AdditionalMetadata.class));
        assertThat(outputToPipesCalled.isEmpty(), is(true));
    }

    private EzSecurityTokenWrapper getEzSecurityToken() {
        return new EzSecurityTokenWrapper(ThriftTestUtils.generateTestSecurityToken("U"));
    }

    private String generateUri() {
        return Generators.randomBasedGenerator().generate().toString();
    }

    private Map<String, AddDocumentResult> generateResultMap(int numDocuments, AddDocumentStatus addDocumentStatus) {
        Map<String,AddDocumentResult> docResultMap = new HashMap<String,AddDocumentResult>();
        for (int ix=0; ix < numDocuments; ++ix) {
            docResultMap.put(generateUri(), new AddDocumentResult(addDocumentStatus));
        }
        return docResultMap;
    }
}