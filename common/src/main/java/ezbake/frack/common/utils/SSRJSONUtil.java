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

package ezbake.frack.common.utils;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import ezbake.base.thrift.SSR;
import ezbake.frack.common.utils.thrift.SSRJSON;

/**
 * This utility method (class) just takes an SSR object and the corresponding
 * thrift object and converts them to a new SSRJSON object. We don't use the
 * JSON version of the object anywhere, so this just cleans up the calling code.
 */
public class SSRJSONUtil {
	/**
	 * 
	 * @param ssr
	 * The ssr parameter is the SSR object that contains data about the object to which it refers:
	 * 	 the Warehouse URI (e.g. CATAGORY://gtdb/x/date),
	 * 	 security (a document classification object ... which contains the visibility string),
	 * 	 title (the name of the object)
	 * 	 snippet (short description of the object)
	 * 	 resultDate (date within the referenced object)
	 * 	 coordinate (geographic location coordinate)
	 * 	 preview (a thumbnail if the object is a mime type)
	 * 
	 * @param thriftObject
	 * The thrift Object to translate to JSON.
	 * 	
	 * @return
	 * 	The SSRJSON result.
	 * 
	 * @throws TException
	 */
	static public SSRJSON fromThriftAndSSR(SSR ssr, TBase thriftObject) throws TException{
		TSerializer ser = new TSerializer(new TSimpleJSONProtocol.Factory());
		return new SSRJSON(ssr, ser.toString(thriftObject));
	}
}
