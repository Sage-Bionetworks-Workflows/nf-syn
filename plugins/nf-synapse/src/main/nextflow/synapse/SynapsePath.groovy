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
import org.sagebionetworks.client.SynapseClientImpl

import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

/**
 * Implements Synapse path object
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
class SynapsePath implements Path {
    public SynapseFileSystem fs

    private Path path

    SynapsePath(SynapseFileSystem fs, String path) {
        this.fs = fs
        this.path = Paths.get(path)
    }

    private SynapsePath(SynapseFileSystem fs, Path path) {
        this.fs = fs
        this.path = path
    }

    private URI getBaseUri() {
        fs?.getBaseUri()
    }

    @Override
    SynapseFileSystem getFileSystem() {
        log.trace 'Inside getFileSystem() from Path'

        return fs
    }

    SynapseClientImpl synapseClient() {
        return fs.getSynapseClient()
    }

    @Override
    boolean isAbsolute() {
        log.trace 'Inside isAbsolute() from Path'

        throw new UnsupportedOperationException("Operation isAbsolute() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path getRoot() {
        log.trace 'Inside getRoot() from Path'

        throw new UnsupportedOperationException("Operation getRoot() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path getFileName() {
        log.trace 'Inside getFileName() from Path'

        throw new UnsupportedOperationException("Operation getFileName() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path getParent() {
        log.trace 'Inside getParent() from Path'

        throw new UnsupportedOperationException("Operation getParent() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    int getNameCount() {
        log.trace 'Inside getNameCount() from Path'

        throw new UnsupportedOperationException("Operation getNameCount() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path getName(int index) {
        log.trace 'Inside getName() from Path'

        throw new UnsupportedOperationException("Operation getName() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        log.trace 'Inside subpath() from Path'

        throw new UnsupportedOperationException("Operation subpath() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean startsWith(Path other) {
        log.trace 'Inside startsWith() from Path'

        throw new UnsupportedOperationException("Operation startsWith() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean endsWith(Path other) {
        log.trace 'Inside endsWith() from Path'

        throw new UnsupportedOperationException("Operation endsWith() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path normalize() {
        log.trace 'Inside normalize() from Path'
        log.trace 'Normalized path: ' + path.normalize()

        return new SynapsePath(fs, path.normalize())
    }

    @Override
    Path resolve(Path other) {
        log.trace 'Inside resolve() from Path'

        throw new UnsupportedOperationException("Operation resolve() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Path relativize(Path other) {
        log.trace 'Inside relativize() from Path'

        throw new UnsupportedOperationException("Operation relativize() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    URI toUri() {
        log.trace 'Inside toUri() from Path'

        return baseUri
    }

    @Override
    String toString() {
        return path.toString()
    }

    @Override
    Path toAbsolutePath() {
        log.trace 'Inside toAbsolutePath() from Path'

        return this
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        log.trace 'Inside toRealPath() from Path'

        throw new UnsupportedOperationException("Operation toRealPath() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        log.trace 'Inside register() from Path'

        throw new UnsupportedOperationException("Operation register() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    int compareTo(Path other) {
        log.trace 'Inside compareTo() from Path'

        throw new UnsupportedOperationException("Operation compareTo() in SynapsePath is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }
}
