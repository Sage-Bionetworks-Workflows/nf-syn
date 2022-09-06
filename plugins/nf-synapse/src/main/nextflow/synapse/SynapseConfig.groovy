/*
 * Copyright 2022, Sage Bionetworks
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
 * limitations under the License.
 */
package nextflow.synapse

import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session

/**
 * Implement system env configs for Synapse FileSystem
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
class SynapseConfig {
    private Map<String,String> sysEnv
    String authToken

    SynapseConfig(Map synConf, Map<String,String> env=null) {
        this.sysEnv = env==null ? new HashMap<String,String>(System.getenv()) : env
        this.authToken = synConf.authToken ?: sysEnv.get('SYNAPSE_AUTH_TOKEN')
    }

    static SynapseConfig getConfig(Session session) {
        if( !session )
            throw new IllegalStateException("Missing Nextflow session")

        new SynapseConfig( (Map)session.config.synapse ?: Collections.emptyMap()  )
    }

    static SynapseConfig getConfig() {
        getConfig(Global.session as Session)
    }

    Map<String,Object> getEnv() {
        Map<String, Object> env = new HashMap<>();
        env.put(SynapseFileSystemProvider.SYNAPSE_AUTH_TOKEN, authToken)

        return env
    }
}
