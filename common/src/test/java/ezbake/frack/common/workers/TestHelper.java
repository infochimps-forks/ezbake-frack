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

import ezbake.base.thrift.Classification;
import ezbake.base.thrift.DateTime;
import ezbake.data.common.TimeUtil;
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.warehaus.Repository;

import java.util.Date;

/**
 * This TestHelper class implements static methods that are useful when testing
 * the associated Worker classes.
 */
public class TestHelper {

    static StreamEvent buildEvent() {
        StreamEvent event = new StreamEvent();
        event.setContent("This is some content".getBytes());
        event.setAuthorization("U");
        event.setDateTime(new Date().toString());
        event.setOrigin("TestHelper");
        return event;
    }

    static DateTime buildDateTime() {
        long time = new Date().getTime();
        return TimeUtil.convertToThriftDateTime(time);
    }

    static Repository buildRepository() {
        Repository repo = new Repository();
        repo.setParsedData("The Parsed Data".getBytes());
        return repo;
    }

    static Classification buildUnclassified() {
        final Classification classification = new Classification();
        classification.setCAPCOString("UNCLASSIFIED");
        return classification;
    }
}