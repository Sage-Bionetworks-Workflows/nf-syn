/*
 * Copyright 2021, Microsoft Corp
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
 * Create Azure path objects for az:// prefixed URIs
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class SynapsePathFactory extends FileSystemPathFactory {

    SynapsePathFactory() {
        log.info 'Inside SynapsePathFactory() from FileSystemPathFactory'
    }

    @Override
    protected Path parseUri(String str) {
        log.info 'Inside parseUri() from FileSystemPathFactory'

        if (!str.startsWith('syn://'))
            return null

        final uri = new URI(null, null, str, null, null)

        log.info 'Starting getOrCreateFileSystemFor from parseUri() from FileSystemPathFactory'
        final fs = FileHelper.getOrCreateFileSystemFor(uri)

        log.info 'Starting provider() from parseUri() from FileSystemPathFactory'
        final provider = fs.provider()

        log.info 'Return getPath() from parseUri() from FileSystemPathFactory'
        return provider.getPath(uri)
    }

    @Override
    protected String toUriString(Path path) {
        log.info 'Inside toUriString() from FileSystemPathFactory'

        return path instanceof SynapsePath ? ((SynapsePath) path).toUriString() : null
    }

    @Override
    protected String getBashLib(Path target) {
        log.info 'Inside getBashLib() from FileSystemPathFactory'

        return null
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        log.info 'Inside getUploadCmd() from FileSystemPathFactory'

        return null
    }
}
