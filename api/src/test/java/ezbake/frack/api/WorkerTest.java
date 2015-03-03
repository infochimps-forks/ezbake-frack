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
import ezbake.frack.core.data.thrift.StreamEvent;

import ezbake.thrift.ThriftUtils;
import org.junit.Test;

public class WorkerTest
{
	public static class TestWorker extends Worker<StreamEvent>
	{
		public TestWorker()
		{
			super(StreamEvent.class);
		}

		@Override
		public void process(Visibility visibility,StreamEvent e)
		{
			System.out.println(e.toString());
		}
	}
	
	@Test
	public void testPump() throws Exception
	{
		TestWorker p = new TestWorker();
		StreamEvent e = new StreamEvent();
		e.setDateTime("foo");
		e.setContent("bar".getBytes());
		System.out.println(e.toString());
		byte [] serial = ThriftUtils.serialize(e);
		p.process(new Visibility().setFormalVisibility("U"), ThriftUtils.deserialize(StreamEvent.class, serial));
	}
}
