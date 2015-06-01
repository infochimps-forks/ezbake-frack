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

package ezbake.frack.eventbus;

import ezbake.base.thrift.Visibility;

import java.io.Serializable;

/**
 * Created by nkhan on 4/1/14.
 */
public class MetaData implements Serializable{

    public String pipeId;
    public Visibility visibility;
    public String errorMsg;
    public boolean isRaw;

    public MetaData(String pipeId, Visibility visibility, String errorMsg, boolean isRaw){
        this.pipeId = pipeId;
        this.visibility = visibility;
        this.errorMsg = errorMsg;
        this.isRaw = isRaw;
    }
}
