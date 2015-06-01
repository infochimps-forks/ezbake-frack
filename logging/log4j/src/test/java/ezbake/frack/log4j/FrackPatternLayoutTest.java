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

package ezbake.frack.log4j;

import com.google.common.base.Strings;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class FrackPatternLayoutTest {
    private static Logger log = Logger.getLogger(FrackPatternLayoutTest.class);

    @Test
    public void testFormat_DefaultFormat() {
        String pipelineID = "test";
        String message = "this is a test log statement";
        Layout layout = new FrackPatternLayout();
        PipelineContext context = new PipelineContext();
        context.setPipelineId(pipelineID);
        PipelineContextProvider.add(context);
        LoggingEvent event = new LoggingEvent(log.getClass().getName(), log, Level.INFO, message, null);
        String formatted = layout.format(event);

        assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(formatted));
        assertEquals("Formatted string is correct", "[" + pipelineID + "] " + message + "\n", formatted);
    }

    @Test
    public void testFormat_DifferentFormat() {
        String pipelineID = "different";
        String message = "WOOOO MESSAGE";
        Layout layout = new FrackPatternLayout("PIPELINE[%f]: %m");
        PipelineContext context = new PipelineContext();
        context.setPipelineId(pipelineID);
        PipelineContextProvider.add(context);
        LoggingEvent event = new LoggingEvent(log.getClass().getName(), log, Level.INFO, message, null);
        String formatted = layout.format(event);

        assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(formatted));
        assertEquals("Formatted string is correct", "PIPELINE[" + pipelineID + "]: " + message, formatted);
    }

    @Test
    public void testFormat_TwoThreads() {
        ExecutorService service = Executors.newFixedThreadPool(2);
        service.execute(new LoggingThread("test_1"));
        service.execute(new LoggingThread("test_2"));
    }

    private static class LoggingThread implements Runnable {
        private String id;

        public LoggingThread(String id) {
            this.id = id;
        }

        @Override
        public void run() {
            String message = "WOOOO MESSAGE";
            Layout layout = new FrackPatternLayout("PIPELINE[%f]: %m");
            PipelineContext context = new PipelineContext();
            context.setPipelineId(id);
            PipelineContextProvider.add(context);
            LoggingEvent event = new LoggingEvent(log.getClass().getName(), log, Level.INFO, message, null);
            String formatted = layout.format(event);

            assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(formatted));
            assertEquals("Formatted string is correct", "PIPELINE[" + id + "]: " + message, formatted);
        }
    }
}
