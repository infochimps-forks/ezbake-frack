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

import com.google.common.base.Preconditions;
import ezbake.base.thrift.Visibility;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class encapsulates a piece of data being passed between pipes, or being broadcast. It contains
 * binary data paired with an Accumulo compatible visibility string which describes the classification
 * of the data. This javadoc explains valid visibility strings:
 * http://accumulo.apache.org/1.4/apidocs/org/apache/accumulo/core/security/ColumnVisibility.html#ColumnVisibility(byte[])
 */
public class Envelope implements Serializable
{
	private String fromPipe;
	private final String messageId;
    private final Visibility visibility;
    private final byte[] data;

	public Envelope(Visibility visibility, byte[] data) throws IOException {
        Preconditions.checkNotNull(visibility, "The visibility can not be null or empty!");

        try {
            // Check the validity of the given visibility string
            new ColumnVisibility(visibility.getFormalVisibility());
        } catch (BadArgumentException e) {
            throw new IOException(String.format("Could not validate visibility string %s", visibility), e);
        }
        
        this.visibility = visibility;
        this.data = data;
        this.messageId = this.createMessageId();
    }
	
    public String getMessageId() {
		return messageId;
	}
    
    public Visibility getVisibility()
    {
        return visibility;
    }
    
    public byte[] getData()
    {
        return data;
    }
    
    private String createMessageId() { 
    	 // We generate a unique message ID 
    	String messageId = "";
        try { 
        	byte[] timeBites = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
	    	MessageDigest digest = MessageDigest.getInstance("SHA-1");
	        digest.reset();
	        digest.update(this.getData());
	        digest.update(timeBites);
	        messageId = new BigInteger(1, digest.digest()).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return messageId;
    }

	public String getFromPipe() {
		return fromPipe;
	}

	public void setFromPipe(String fromPipe) {
		this.fromPipe = fromPipe;
	}
}
