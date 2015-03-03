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

package ezbake.frack.eventbus;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.frack.api.*;
import ezbake.frack.api.impl.EventBusPipelineSubmitter;
import ezbake.frack.api.impl.GeneratorEventHandler;
import ezbake.frack.api.impl.WorkerEventHandler;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import ezbake.frack.core.data.thrift.StreamEvent;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventBusTest {
    private static Set<String> processResult;
    private static Map<String, Integer> processDataMapWorkerOne;
    private static Map<String, Integer> processDataMapWorkerTwo;


    //Change this variable to use a different temp directory.
    private static final String BASE_TEMP_DIR = System.getProperty("user.home")
            + File.separator + "Desktop"
            + File.separator + "data";

    private static final String TEMP_DATA_DIR = BASE_TEMP_DIR
            + File.separator + "quarantine";

    private static final String TEMP_REPLAY_DIR = BASE_TEMP_DIR
            + File.separator + "replay";

    private static final String STRING_ONE = "test string one";
    private static final String STRING_TWO = "test string two";
    private static final String STRING_THREE = "test string three";
    private static final String STRING_FOUR = "test string four";
    private static final String STRING_FIVE = "test string five";

    private static final String PIPE_LINE_ID = "test-pipeline";
    private static final String GENERATOR_ID = "test-generator";
    private static final String WORKER_ONE_ID = "test-worker-one";
    private static final String WORKER_TWO_ID = "test-worker-two";

    @Before
    public void setupTest() {
        processResult = Sets.newHashSet();
        processDataMapWorkerOne = new HashMap<>();
        processDataMapWorkerTwo = new HashMap<>();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        File dataDir = new File(BASE_TEMP_DIR);
        FileUtils.deleteDirectory(dataDir);
    }

    static class TestWorker extends Worker<StreamEvent>
    {
        public TestWorker() {
            super(StreamEvent.class);
        }

        @Override
        public void process(Visibility visibility, StreamEvent streamEvent) {
            String resultString = new String(streamEvent.getContent()) + "!!!!";
            processResult.add(visibility.getFormalVisibility() + " " + resultString);
        }
    }

    static class TestWorkerOne extends Worker<StreamEvent>
    {
        public TestWorkerOne() {
            super(StreamEvent.class);
        }

        @Override
        public void process(Visibility visibility, StreamEvent streamEvent) {
            String data = new String(streamEvent.getContent());

            System.out.println("Worker called with " + new String(streamEvent.getContent()));
            if(!processDataMapWorkerOne.containsKey(data)) {
                //Its the first pass
                processDataMapWorkerOne.put(data, 1);
                try {
                    sendObjectToQuarantine(streamEvent, new Visibility().setFormalVisibility("U"), "Error message", null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                //Data is being replayed by quarantine generator
                int processCount = processDataMapWorkerOne.get(data);
                processDataMapWorkerOne.put(data, ++processCount);
            }
        }
    }

    /**
     * This worker simply calls out to pipe on the data it receives
     */
    static class TestWorkerTwo extends Worker<StreamEvent> {

        public TestWorkerTwo() {
            super(StreamEvent.class);
        }

        @Override
        public void process(Visibility visibility, StreamEvent event) {
            System.out.println("Called worker two with " + new String(event.getContent()));
            String data = new String(event.getContent());

            if(!processDataMapWorkerTwo.containsKey(data)){
                //Its the first pass
                processDataMapWorkerTwo.put(data, 1);
            }else{
                //Data is being replayed by quarantine generator
                int processCount = processDataMapWorkerTwo.get(data);
                processDataMapWorkerTwo.put(data, ++processCount);
            }
        }
    }

    static class TestGenerator extends Generator<StreamEvent> {
        String[] strings = {STRING_ONE, STRING_TWO, STRING_THREE, STRING_FOUR, STRING_FIVE};
        boolean done;
        private int genCounter = 0;

        @Override
        public void generate() {
            if (!done) {
                for (String string : strings) {
                    StreamEvent event = new StreamEvent();
                    event.setContent(string.getBytes());
                    event.setDateTime(Long.toString(System.currentTimeMillis()));
                    event.setOrigin("test generator");
                    if(genCounter < 3) {
                        try {
                            outputToPipes(new Visibility().setFormalVisibility("U"), event);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (genCounter == 3) {
                        try {
                            sendRawToQuarantine(string.getBytes(), new Visibility().setFormalVisibility("U"), "Some error message", null);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    genCounter++;
                }
                done = true;
            }
        }

        @Override
        public void initialize(Properties props) {
            done = false;
        }
    }

    @BeforeClass
    public static void setup() {
        processResult = Sets.newHashSet();
    }

//	@Test
//	public void testEventBus() throws Exception {
//        Properties props = new Properties();
//        EventBus bus = new EventBus(props);
//        props.setProperty(EzBakePropertyConstants.THRIFT_USE_SSL, Boolean.toString(false));
//        props.setProperty(EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
//
//        GeneratorEventHandler generatorHandler = new GeneratorEventHandler(bus, GENERATOR_ID,
//                new TestGenerator(), PIPE_LINE_ID, props, 1);
//        bus.subscribe(GENERATOR_ID, WORKER_ONE_ID, StreamEvent.class);
//        WorkerEventHandler workerHandler = new WorkerEventHandler(bus, WORKER_ONE_ID, new TestWorker(), PIPE_LINE_ID,
//                props, 1);
//
//        ExecutorService service = Executors.newFixedThreadPool(2);
//        service.execute(generatorHandler.getThreads().get(0));
//        service.execute(workerHandler.getThreads().get(0));
//        Thread.sleep(1000);
//        generatorHandler.shutdown();
//        workerHandler.shutdown();
//
//        // Make sure that the transferred content is all in the result set
//        assertEquals("Three items in result set", 3, processResult.size());
//        assertTrue("Test string one in result", processResult.contains("U" + " " + STRING_ONE + "!!!!"));
//        assertTrue("Test string two in result", processResult.contains("U" + " " + STRING_TWO + "!!!!"));
//        assertTrue("Test string three in result", processResult.contains("U" + " " + STRING_THREE + "!!!!"));
//    }


//    /**
//     * This test creates failed data which is placed
//     * in the following configurable (see TEMP_DATA_DIR) directory
//     * (directory is created if it is not present)
//     *  /Users/{username}/Desktop/data/quarantine/
//     * You will have to manually delete this data after testing.
//     * @throws Exception
//     */
//    @Test
//    public void testSendToQuarantine() throws Exception {
//        Properties props = new Properties();
//        props.setProperty(EventBus.QUARANTINE_DIR, TEMP_DATA_DIR);
//        EventBus bus = new EventBus(props);
//        //Make sure the data is in quarantine
//        File qDir = new File(TEMP_DATA_DIR + File.separator + PIPE_LINE_ID);
//        if(qDir.exists())
//            FileUtils.cleanDirectory(qDir);
//
//        GeneratorEventHandler generatorHandler = new GeneratorEventHandler(bus, GENERATOR_ID, new TestGenerator(),
//                PIPE_LINE_ID, props, 1);
//        bus.subscribe(GENERATOR_ID, WORKER_ONE_ID, StreamEvent.class);
//        WorkerEventHandler workerHandler = new WorkerEventHandler(bus, WORKER_ONE_ID, new TestWorkerOne(),
//                PIPE_LINE_ID, props, 1);
//
//        ExecutorService service = Executors.newFixedThreadPool(2);
//        service.execute(generatorHandler.getThreads().get(0));
//        service.execute(workerHandler.getThreads().get(0));
//        Thread.sleep(1000);
//
//        QuarantineEventHandler.GenericExtFilter filter = new QuarantineEventHandler.GenericExtFilter(".meta");
//        int testGeneratorCounter = 0;
//        int testWorkerCounter = 0;
//        for(String file : qDir.list(filter)) {
//            File f = new File(qDir.getAbsolutePath() + File.separator + file);
//            ObjectInputStream in = new ObjectInputStream(new FileInputStream(f));
//            MetaData metaData = (MetaData) in.readObject();
//            if (metaData.pipeId.equals(GENERATOR_ID)){
//                testGeneratorCounter++;
//            } else if (metaData.pipeId.equals(WORKER_ONE_ID)) {
//                String dataFile = qDir.getAbsolutePath() + File.separator +
//                        file.replace(EventBus.EXT_META, EventBus.EXT_SERIALIZED);
//                //Check that for every meta file sent to quarantine by worker, there is a data file
//                assertTrue(new File(dataFile).exists());
//                assertTrue(!metaData.isRaw);
//                testWorkerCounter++;
//            }
//        }
//        //Generator sends to quarantine 1 time
//        assertEquals(1, testGeneratorCounter);
//        //Output to pipe is invoked 3 times (See TestGenerator)
//        // which invokes the testOneWorker
//        // 3 times which sends all data to quarantine
//        assertEquals(3, testWorkerCounter);
//        generatorHandler.shutdown();
//        workerHandler.shutdown();
//    }
//
//    @Test
//    public void testQuarantine() throws IOException, InterruptedException {
//        Map<String, Integer> processDataMapWorkerOne = Maps.newHashMap();
//        Map<String, Integer> processDataMapWorkerTwo = Maps.newHashMap();
//        String quarantineDirPath = TEMP_DATA_DIR + File.separator + PIPE_LINE_ID;
//        String replayDirPath = TEMP_REPLAY_DIR + File.separator + PIPE_LINE_ID;
//
//        Properties props = new Properties();
//        props.setProperty(QuarantineEventHandler.REPLAY_DIR, replayDirPath);
//        props.setProperty(EventBus.QUARANTINE_DIR, TEMP_DATA_DIR);
//
//        File quarantineTestPipelineDir = new File(quarantineDirPath);
//        File replayTestPipelineDir = new File(replayDirPath);
//
//
//        //Quarantine directory is emptied or created
//        //depending on if it already exists
//        cleanOrMake(quarantineTestPipelineDir);
//        cleanOrMake(replayTestPipelineDir);
//
//        //Create a pipeline with one generator and two workers
//        PipelineContext context = new PipelineContext();
//        context.setPipelineId(PIPE_LINE_ID);
//        context.setProps(props);
//
//        PipelineContextProvider.add(context);
//
//        //Create the generator
//        TestGenerator gen = new TestGenerator();
//        //Create workers
//        TestWorkerOne worker = new TestWorkerOne();
//        TestWorkerTwo workerTwo = new TestWorkerTwo();
//
//        Pipeline pipeline = new Pipeline();
//        pipeline.addWorker(WORKER_ONE_ID, worker);
//        pipeline.addWorker(WORKER_TWO_ID, workerTwo);
//        pipeline.addGenerator(GENERATOR_ID, gen);
//
//        //Add connection between the generator and worker
//        pipeline.addConnection(GENERATOR_ID, WORKER_ONE_ID);
//        pipeline.addConnection(GENERATOR_ID, WORKER_TWO_ID);
//
//        //Submit the pipe line
//        EventBusPipelineSubmitter submitter = new EventBusPipelineSubmitter();
//        submitter.submit(pipeline);
//
//        Thread.sleep(2000);
//
//        //Move the data over to replay directory so the quarantine generator
//        //can pick it up
//        for(File f : quarantineTestPipelineDir.listFiles()){
//            FileUtils.moveFileToDirectory(f, replayTestPipelineDir, false);
//        }
//        //Allow quarantine generator to process data
//        Thread.sleep(3000);
//
//        for(String data : processDataMapWorkerOne.keySet()){
//            int workerOnePlayCount = processDataMapWorkerOne.get(data);
//            int workerTwoPlayCount = processDataMapWorkerTwo.get(data);
//            //Each item in worker one should have a play count of 2 since the worker
//            //sends it to quarantine once and is replayed by the quarantine handler
//            //once
//            assertTrue(workerOnePlayCount == 2);
//
//            //Make sure that the data is not being replayed on worker two
//            //i.e. it is only played once
//            assertTrue(workerTwoPlayCount == 1);
//        }
//    }
}
