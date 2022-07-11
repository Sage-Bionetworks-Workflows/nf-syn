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

import nextflow.file.FileHelper

import java.nio.file.FileSystem
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileSystemPathFactory
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
    protected Path parseUri(String uri) {
        log.info 'Inside parseUri() from FileSystemPathFactory'

        if( !uri.startsWith('syn://') )
            return null

        final cfg = new HashMap<>();

        // find the related file system
        final fs = getFileSystem(uri0(uri), cfg)

        // resulting az path
        return fs.getPath('/')
    }

    private URI uri0(String uri) {
        log.info 'Inside uri0() from FileSystemPathFactory'

        // note: this is needed to allow URI to handle curly brackets characters
        // see https://github.com/nextflow-io/nextflow/issues/1969
        new URI(null, null, uri, null, null)
    }

    protected FileSystem getFileSystem(URI uri, Map env) {
        log.info 'Inside getFileSystem() from FileSystemPathFactory'

        final bak = Thread.currentThread().getContextClassLoader()
        // NOTE: setting the context classloader to allow loading azure deps via java ServiceLoader
        // see
        //  com.azure.core.http.HttpClientProvider
        //  com.azure.core.http.netty.implementation.ReactorNettyClientProvider
        //
        try {
            final loader = SynapsePlugin.class.getClassLoader()
            log.debug "+ Setting context class loader to=$loader - previous=$bak"
            Thread.currentThread().setContextClassLoader(loader)
            return FileHelper.getOrCreateFileSystemFor(uri, env)
        }
        finally {
            Thread.currentThread().setContextClassLoader(bak)
        }
    }

    @Override
    protected String toUriString(Path path) {
        log.info 'Inside toUriString() from FileSystemPathFactory'

        return path instanceof SynapsePath ? ((SynapsePath)path).toUriString() : null
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
