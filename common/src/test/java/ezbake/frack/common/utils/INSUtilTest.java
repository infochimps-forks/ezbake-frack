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

package ezbake.frack.common.utils;

import com.google.common.collect.Sets;
import ezbake.frack.api.Pipeline;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.powermock.api.mockito.PowerMockito.*;
import static org.junit.Assert.*;

@PrepareForTest(INSUtil.class)
@RunWith(PowerMockRunner.class)
public class INSUtilTest {

    @Test
    public void getINSInfoTest() throws Exception {
        String appId = "test";
        String feedName = "feed";
        String topic = "testTopic";
        String uriPrefix = "DEV://test";

        // Mock the client and pool
        InternalNameService.Client mockClient = when(mock(InternalNameService.Client.class).getTopicsForFeed(appId, feedName)).thenReturn(Sets.newHashSet(topic, "SSR")).getMock();
        when(mockClient.getSystemTopics()).thenReturn(Sets.newHashSet("GEOSPATIAL-FEATURE", "GEOSPATIAL-FEATURE-LAYER", "GEOSPATIAL-LAYER", "SSR"));
        doReturn(uriPrefix).when(mockClient).getURIPrefix(appId, feedName);
        ThriftClientPool mockPool = when(mock(ThriftClientPool.class).getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class)).thenReturn(mockClient).getMock();
        whenNew(ThriftClientPool.class).withAnyArguments().thenReturn(mockPool);

        // Mock the config helper
        EzBakeApplicationConfigurationHelper mockHelper = when(mock(EzBakeApplicationConfigurationHelper.class).getSecurityID()).thenReturn(appId).getMock();
        whenNew(EzBakeApplicationConfigurationHelper.class).withAnyArguments().thenReturn(mockHelper);

        // Mock the pipeline
        Pipeline mockPipeline = when(mock(Pipeline.class).getProperties()).thenReturn(new Properties()).getMock();

        INSUtil.INSInfo insInfo = INSUtil.getINSInfo(mockPipeline, feedName);
        assertNotNull("insInfo isn't null", insInfo);
        assertEquals("Correct URI prefix", uriPrefix, insInfo.getUriPrefix());
        assertEquals("Only one topic returned", 1, insInfo.getTopics().size());
        assertTrue("Set of topics contains test topic", insInfo.getTopics().contains(topic));
    }
}
