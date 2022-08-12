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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.file.FileSystemPathFactory

import java.nio.file.Path

/**
 * Create Synapse path objects for syn:// prefixed URIs
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
@CompileStatic
class SynapsePathFactory extends FileSystemPathFactory {

    SynapsePathFactory() {
        log.trace 'Inside SynapsePathFactory() from FileSystemPathFactory'
    }

    @Override
    protected Path parseUri(String str) {
        log.trace 'Inside parseUri() from FileSystemPathFactory'

        if (!str.startsWith('syn://'))
            return null

        final uri = new URI(null, null, str, null, null)
        final env = SynapseConfig.getConfig().getEnv()

        log.trace 'Starting getOrCreateFileSystemFor from parseUri() from FileSystemPathFactory'
        final fs = FileHelper.getOrCreateFileSystemFor(uri, env)

        log.trace 'Starting provider() from parseUri() from FileSystemPathFactory'
        final provider = fs.provider()

        log.trace 'Return getPath() from parseUri() from FileSystemPathFactory'
        return provider.getPath(uri)
    }

    @Override
    protected String toUriString(Path path) {
        log.trace 'Inside toUriString() from FileSystemPathFactory'

        return path instanceof SynapsePath ? ((SynapsePath) path).toUriString() : null
    }

    @Override
    protected String getBashLib(Path target) {
        log.trace 'Inside getBashLib() from FileSystemPathFactory'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        log.trace 'Inside getUploadCmd() from FileSystemPathFactory'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }
}
