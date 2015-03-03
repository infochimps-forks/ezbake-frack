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

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Generator;
import ezbake.frack.core.data.thrift.StreamEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RandomSentenceGenerator extends Generator<StreamEvent> {

    private String[] sentences = new String[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"};

	@Override
	public void generate() {
		StreamEvent e = new StreamEvent();
		Random rand = new Random();
		
	    String sentence = sentences[rand.nextInt(sentences.length)];
	    e.dateTime = getCurrentTimeStamp();
	    e.content = ByteBuffer.wrap(sentence.getBytes());
        try {
            outputToPipes(new Visibility().setFormalVisibility("U"), e);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }
	
	public static String getCurrentTimeStamp() 
	{
    	SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
    	Date now = new Date();
    	String strDate = sdfDate.format(now);
    	return strDate;
	}
}
