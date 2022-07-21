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
    private static final String[] EMPTY = []

    public SynapseFileSystem fs

    private Path path

    SynapsePath(SynapseFileSystem fs, String path) {
        this(fs, path, EMPTY)
    }

    SynapsePath(SynapseFileSystem fs, String path, String[] more) {
        this.fs = fs
        this.path = Paths.get(path)
    }

    private SynapsePath(SynapseFileSystem fs, Path path, String query = null) {
        this.fs = fs
        this.path = path
    }

    private URI getBaseUri() {
        fs?.getBaseUri()
    }

    @Override
    SynapseFileSystem getFileSystem() {
        log.info 'Inside getFileSystem() from Path'

        return fs
    }

    @Override
    boolean isAbsolute() {
        log.info 'Inside isAbsolute() from Path'

        return false
    }

    @Override
    Path getRoot() {
        log.info 'Inside getRoot() from Path'

        return null
    }

    @Override
    Path getFileName() {
        log.info 'Inside getFileName() from Path'

        return null
    }

    @Override
    Path getParent() {
        log.info 'Inside getParent() from Path'

        return null
    }

    @Override
    int getNameCount() {
        log.info 'Inside getNameCount() from Path'

        return 0
    }

    @Override
    Path getName(int index) {
        log.info 'Inside getName() from Path'

        return null
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        log.info 'Inside subpath() from Path'

        return null
    }

    @Override
    boolean startsWith(Path other) {
        log.info 'Inside startsWith() from Path'

        return false
    }

    @Override
    boolean endsWith(Path other) {
        log.info 'Inside endsWith() from Path'

        return false
    }

    @Override
    Path normalize() {
        log.info 'Inside normalize() from Path'
        log.info 'Normalized path: ' + path.normalize()

        return new SynapsePath(fs, path.normalize())
    }

    @Override
    Path resolve(Path other) {
        log.info 'Inside resolve() from Path'

        return null
    }

    @Override
    Path relativize(Path other) {
        log.info 'Inside relativize() from Path'

        return null
    }

    @Override
    URI toUri() {
        log.info 'Inside toUri() from Path'

        return baseUri
    }

    @Override
    String toString() {
        return path.toString()
    }

    @Override
    Path toAbsolutePath() {
        log.info 'Inside toAbsolutePath() from Path'

        return this
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        log.info 'Inside toRealPath() from Path'

        return null
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        log.info 'Inside register() from Path'

        return null
    }

    @Override
    int compareTo(Path other) {
        log.info 'Inside compareTo() from Path'

        return 0
    }
}
