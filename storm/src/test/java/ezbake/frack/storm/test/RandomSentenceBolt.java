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

package ezbake.frack.storm.test;

import java.io.IOException;

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.core.data.thrift.StreamEvent;

public class RandomSentenceBolt extends Worker<StreamEvent> {
	
	public RandomSentenceBolt() {
		super(StreamEvent.class);
	}

	@Override
	public void process(Visibility visibility, StreamEvent e) {
		byte [] data = e.getContent();
		
		String str = new String(data);    
		System.out.println("Bolt output: " + str);
	}
}
