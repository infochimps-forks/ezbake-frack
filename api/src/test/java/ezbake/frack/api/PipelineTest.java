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

import com.google.common.collect.Lists;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.PropertiesConfigurationLoader;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.core.data.thrift.StreamEvent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PipelineTest
{

	public static class SampleWorker1 extends Worker<StreamEvent>
	{
        private List<String> processedStrings;

		public SampleWorker1()
		{
			super(StreamEvent.class);
            processedStrings = Lists.newArrayList();
        }
		
		@Override
		public void process(Visibility visibility, StreamEvent object)
		{
			processedStrings.add(new String(object.getContent()));
		}

        public List<String> getProcessedStrings() {
            return processedStrings;
        }
	}

    public static class SampleWorker2 extends Worker<StreamEvent>
    {
        private List<String> processedStrings;

        public SampleWorker2()
        {
            super(StreamEvent.class);
            processedStrings = Lists.newArrayList();
        }

        @Override
        public void process(Visibility visibility, StreamEvent object)
        {
            processedStrings.add(new String(object.getContent()) + "!!!");
        }

        public List<String> getProcessedStrings() {
            return processedStrings;
        }
    }

	public static class MyGenerator extends Generator<StreamEvent>
	{
        String[] stringsToGenerate;

        public MyGenerator(String[] strings) {
            stringsToGenerate = strings;
        }
        
		@Override
		public void generate() {
            for (String string : stringsToGenerate) {
                StreamEvent e = new StreamEvent();
                e.setDateTime(new Date().toString());
                e.setContent(string.getBytes());
                try {
                    outputToPipes(new Visibility().setFormalVisibility("U"), e);
                } catch (IOException ioe) {
                    // handle the exception, possibly send to quarantine
                }
            }
        }
	}

    @Before
    public void setup() {
        PipelineContext context = new PipelineContext();
        context.setPipelineId("test");
        context.setProps(new Properties());
        PipelineContextProvider.add(context);
    }

    @Test
    public void testPipelineInputs() throws IOException
    {
        Pipeline pipeline = new Pipeline();
        List<String> generatorIds = Lists.newArrayList("g1", "g2", "g3", "g4");
        pipeline.addWorker("w", new SampleWorker1());
        for (String id : generatorIds) {
            pipeline.addGenerator(id, new MyGenerator(new String[]{"test"}));
            pipeline.addConnection(id, "w");
        }

        List<Pipeline.PipeInfo> infos = pipeline.getPipeInfos();
        assertEquals("Correct number of pipes", 5, infos.size());

        for (Pipeline.PipeInfo info : infos) {
            if (info.getPipeId().equals("w")) {
                assertEquals("Correct number of inputs", 4, info.getInputs().size());
                assertEquals("Input list is correct", generatorIds, info.getInputs());
            }
        }
    }

    @Test
    public void testPipelineContext() {
        String id = "test_id";
        Properties props = new Properties();
        props.setProperty("test.property", "12345");
        PipelineContext context = new PipelineContext();
        context.setPipelineId(id);
        context.setProps(props);
        PipelineContextProvider.add(context);
        Pipeline p = new Pipeline();

        assertEquals("Pipeline has correct ID", id, p.getId());
        assertEquals("Configuration has correct test key", "12345", p.getProperties().getProperty("test.property"));
    }

    @Test(expected=IllegalStateException.class)
    public void testAddConnection_FromPipeDoesntExist() {
        Pipeline p = new Pipeline();
        p.addWorker("w", new SampleWorker1());
        p.addConnection("g", "w");
    }

    @Test(expected=IllegalStateException.class)
    public void testAddConnection_ToPipeDoesntExist() {
        Pipeline p = new Pipeline();
        p.addGenerator("g", new MyGenerator(new String[]{"whatever"}));
        p.addConnection("g", "w");
    }
	
	@Test
	public void basicPipelineTest() throws IOException
	{
		Pipeline pl = new Pipeline();
        String[] inputs = new String[]{"test", "one"};
		pl.addGenerator("myNozzle", new MyGenerator(inputs));
        SampleWorker1 worker = new SampleWorker1();
		pl.addWorker("myWorker", worker);
		pl.addConnection("myNozzle", "myWorker");
		new TestPipelineSubmitter(1000, 1).submit(pl);

        List<String> results = worker.getProcessedStrings();
        String[] resultArray = results.toArray(new String[2]);
        assertArrayEquals("Result array equals input array", inputs, resultArray);
	}

    @Test
    public void branchingPipelineTest() throws IOException
    {
        Pipeline pl = new Pipeline();
        String[] inputs = new String[]{"test", "one", "two", "three"};
        pl.addGenerator("myNozzle", new MyGenerator(inputs));
        SampleWorker1 worker1 = new SampleWorker1();
        SampleWorker2 worker2 = new SampleWorker2();
        pl.addWorker("myWorker1", worker1);
        pl.addWorker("myWorker2", worker2);
        pl.addConnection("myNozzle", "myWorker1");
        pl.addConnection("myNozzle", "myWorker2");
        new TestPipelineSubmitter(1000, 1).submit(pl);

        String[] resultArray = worker1.getProcessedStrings().toArray(new String[4]);
        assertArrayEquals("Worker1 result array equals input array", inputs, resultArray);

        String[] worker2Expected = new String[4];
        int index = 0;
        for (String string : inputs) {
            worker2Expected[index++] = string + "!!!";
        }
        String[] worker2Results = worker2.getProcessedStrings().toArray(new String[4]);
        assertArrayEquals("Worker2 result array equals input array", worker2Expected, worker2Results);
    }

}
