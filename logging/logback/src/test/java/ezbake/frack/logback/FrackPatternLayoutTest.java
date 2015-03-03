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

package ezbake.frack.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.base.Strings;
import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class FrackPatternLayoutTest {
    private static Logger rootLogger = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static LoggerContext loggerContext = rootLogger.getLoggerContext();

    @BeforeClass
    public static void setup() {
        loggerContext.reset();
    }

    @Test
    public void testFormat_SimpleFormat() {
        String pipelineID = "test";
        String message = "message";
        FrackPatternLayout layout = new FrackPatternLayout();
        layout.setPattern(": %message");
        layout.setContext(loggerContext);
        layout.start();

        PipelineContext context = new PipelineContext();
        context.setPipelineId(pipelineID);
        PipelineContextProvider.add(context);
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setMessage(message);

        String result = layout.doLayout(event);

        assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(result));
        assertEquals("Layout produces correct string", "[" + pipelineID + "]: " + message, result);
    }

    @Test
    public void testFormat_DifferentFormat() {
        String pipelineID = "test";
        String message = "another message";
        FrackPatternLayout layout = new FrackPatternLayout();
        layout.setPattern("[%thread]: %message%n");
        layout.setContext(loggerContext);
        layout.start();

        PipelineContext context = new PipelineContext();
        context.setPipelineId(pipelineID);
        PipelineContextProvider.add(context);
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setMessage(message);

        String result = layout.doLayout(event);

        assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(result));
        assertEquals("Layout produces correct string", "[" + pipelineID + "][main]: " + message + "\n", result);
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
            FrackPatternLayout layout = new FrackPatternLayout();
            layout.setPattern(": %message");
            layout.setContext(loggerContext);
            layout.start();

            PipelineContext context = new PipelineContext();
            context.setPipelineId(id);
            PipelineContextProvider.add(context);
            LoggingEvent event = new LoggingEvent();
            event.setLevel(Level.INFO);
            event.setMessage(message);
            String result = layout.doLayout(event);

            assertFalse("Formatted string should not be null or empty", Strings.isNullOrEmpty(result));
            assertEquals("Formatted string is correct", "[" + id + "]: " + message, result);
        }
    }
}
