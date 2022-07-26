package nextflow.synapse

import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session

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
        if( !authToken )
            throw new IllegalArgumentException("Missing SYNAPSE_AUTH_TOKEN")

        Map<String, Object> props = new HashMap<>();
        props.put(SynapseFileSystemProvider.SYNAPSE_AUTH_TOKEN, authToken)

        return props
    }
}
