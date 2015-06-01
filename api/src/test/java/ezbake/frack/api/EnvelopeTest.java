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

import ezbake.base.thrift.Visibility;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by eperry on 12/19/13.
 */
public class EnvelopeTest {

    @Test
    public void testConstructor_ValidVisibility() throws IOException {
        // Just making sure that we don't get an IOException
        Envelope env = new Envelope(new Visibility().setFormalVisibility("U&FOUO&(A|B|C)"), "test".getBytes());
    }

    @Test(expected = IOException.class)
    public void testConstructor_InvalidVisibility() throws IOException {
        // Just making sure that we don't get an IOException
        Envelope env = new Envelope(new Visibility().setFormalVisibility("THIS||ISNT&@$VALID"), "test".getBytes());
    }
}
