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
 * Contains configuration options used by the PipelineSubmitter class in order to properly submit
 * Pipelines to the underlying ingest system.
 */
public class SubmitterConfiguration {
    private String ingestSystem;
    private String pathToJar;
    private String builderClass;
    private String userPropertiesFolder;
    private String pipelineId;
    private String overrideConfigDir;
    private String sslDir;
    private boolean isProduction;

    public String getIngestSystem() {
        return ingestSystem;
    }

    public SubmitterConfiguration setIngestSystem(String ingestSystem) {
        this.ingestSystem = ingestSystem;
        return this;
    }

    public String getPathToJar() {
        return pathToJar;
    }

    public SubmitterConfiguration setPathToJar(String pathToJar) {
        this.pathToJar = pathToJar;
        return this;
    }

    public String getBuilderClass() {
        return builderClass;
    }

    public SubmitterConfiguration setBuilderClass(String builderClass) {
        this.builderClass = builderClass;
        return this;
    }

    public boolean isProduction() {
        return isProduction;
    }

    public SubmitterConfiguration setProduction(boolean production) {
        isProduction = production;
        return this;
    }

    public String getOverrideConfigDir() {
        return overrideConfigDir;
    }

    public SubmitterConfiguration setOverrideConfigDir(String overrideConfigDir) {
        this.overrideConfigDir = overrideConfigDir;
        return this;
    }

    public String getUserPropertiesFolder() {
        return userPropertiesFolder;
    }

    public SubmitterConfiguration setUserPropertiesFolder(String userPropertiesFolder) {
        this.userPropertiesFolder = userPropertiesFolder;
        return this;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public SubmitterConfiguration setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
        return this;
    }

    public String getSslDir() {
        return sslDir;
    }

    public SubmitterConfiguration setSslDir(String sslDir) {
        this.sslDir = sslDir;
        return this;
    }
}
