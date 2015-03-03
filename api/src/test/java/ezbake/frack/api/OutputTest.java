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

import com.google.common.collect.Multimap;
import ezbake.base.thrift.Visibility;
import ezbake.frack.core.data.thrift.StreamEvent;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by eperry on 12/20/13.
 */
public class OutputTest {
    private static class TestOutputter implements Runnable {
        private String[] stringsToOutput;
        private Output<StreamEvent> output;

        public TestOutputter(String[] stringsToOutput, Output<StreamEvent> output) {
            this.stringsToOutput = stringsToOutput;
            this.output = output;
        }

        @Override
        public void run() {
            for (String toOutput : stringsToOutput) {
                StreamEvent event = new StreamEvent(new Date().toString(), ByteBuffer.wrap(toOutput.getBytes()));
                try {
                    output.toPipes(new Visibility().setFormalVisibility("U"), event);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    public void testOutputToPipesAndRetrieveData() throws TException, IOException, ClassNotFoundException {
        String[] strings = new String[]{"hello", "this", "is", "a", "test"};
        Output<StreamEvent> output = new Output<StreamEvent>();
        SimplePublisher p = new SimplePublisher();
        output.registerPublisher(p);
        TestOutputter outputter = new TestOutputter(strings, output);
        outputter.run();
        Multimap<Class<? extends Serializable>, Envelope> data = p.getDataBetweenPipes();
        Collection<Envelope> envs = data.get (StreamEvent.class);

        // Make sure we have the right amount of strings
        assertEquals("Retrieved the correct amount of objects from Output", strings.length, envs.size());

        // Make sure there are no objects anymore
        assertTrue("No objects left in Output", p.getDataBetweenPipes().isEmpty());

        String[] resultStrings = new String[strings.length];
        int lastIndex = 0;
        for (Envelope e : envs) {
            ByteArrayInputStream bais = new ByteArrayInputStream(e.getData());
            ObjectInputStream ois = new ObjectInputStream(bais);
            ois.close();
            StreamEvent event = (StreamEvent)ois.readObject();
            resultStrings[lastIndex++] = new String(event.getContent());
        }

        assertArrayEquals("Input strings match output strings", strings, resultStrings);
    }
}
