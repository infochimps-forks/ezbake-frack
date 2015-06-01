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

package ezbake.frack.submitter.util;

/**
 * Created by eperry on 1/21/14.
 */
public enum SubmitterClass {
    STORM("ezbake.frack.storm.core.StormPipelineSubmitter", "ezbake.frack.submitter.systems.StormPipelineKiller"),
    EVENTBUS("ezbake.frack.api.impl.EventBusPipelineSubmitter", "ezbake.frack.submitter.systems.EventBusPipelineKiller");

    private String className;
    private String killerClassName;

    private SubmitterClass(String className, String killerClassName) {
        this.className = className;
        this.killerClassName = killerClassName;
    }

    public static String getSubmitterClass(String clazz) {
        return SubmitterClass.valueOf(clazz.toUpperCase()).getClassName();
    }

    public static String getKillerClass(String clazz) {
        return SubmitterClass.valueOf(clazz.toUpperCase()).getKillerClassName();
    }

    public String getClassName() {
        return className;
    }

    public String getKillerClassName() {
        return killerClassName;
    }
}
