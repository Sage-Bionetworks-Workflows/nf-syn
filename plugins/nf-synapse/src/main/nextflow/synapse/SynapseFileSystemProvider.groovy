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

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemNotFoundException
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

/**
 * Implements NIO File system provider for Synapse Entity Storage
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
class SynapseFileSystemProvider extends FileSystemProvider {
    private Map<URI, FileSystem> fileSystemMap = new LinkedHashMap<>(20)
    public static final String SYNAPSE_AUTH_TOKEN = 'SYNAPSE_AUTH_TOKEN'
    private Map<String,String> env = new HashMap<>(System.getenv())
    private String authToken = null

    @Override
    String getScheme() {
        log.info 'Inside getScheme() from FileSystemProvider'

        return "syn"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> config) throws IOException {
        log.info 'Inside newFileSystem() from FileSystemProvider'

        final authToken = config.get(SYNAPSE_AUTH_TOKEN) as String
        final scheme = uri.scheme.toLowerCase()

        if (scheme != this.getScheme())
            throw new IllegalArgumentException("Not a valid ${getScheme().toUpperCase()} scheme: $scheme")

        final base = uri

        log.info 'Inside newFileSystem() from FileSystemProvider'

        if (fileSystemMap.containsKey(base))
            throw new IllegalStateException("File system `$base` already exists")

        if(authToken) {
            this.authToken = authToken
        }

        return new SynapseFileSystem(this, base)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        log.info 'Inside getFileSystem(URI uri) from FileSystemProvider'

        getFileSystem(uri, false)
    }

    FileSystem getFileSystem(URI uri, boolean canCreate) {
        log.info 'Inside getFileSystem(URI uri, boolean canCreate) from FileSystemProvider'
        log.info 'getFileSystem() from FileSystemProvider -> URI: ' + uri

        assert fileSystemMap != null

        final scheme = uri.scheme.toLowerCase()

        if (scheme != this.getScheme())
            throw new IllegalArgumentException("Not a valid ${getScheme().toUpperCase()} scheme: $scheme")

        final key = uri

        if (!canCreate) {
            FileSystem result = fileSystemMap[key]
            if (result == null)
                throw new FileSystemNotFoundException("File system not found: $key")
            return result
        }

        synchronized (fileSystemMap) {
            FileSystem result = fileSystemMap[key]
            if (result == null) {
                result = newFileSystem(uri, env)
                fileSystemMap[key] = result
            }
            return result
        }
    }

    @Override
    Path getPath(URI uri) {
        log.info 'Inside getPath() from FileSystemProvider'

        def path = uri.getSchemeSpecificPart()
        path = path.substring(1)

        log.info 'from FileSystemProvider -> getPath() -> Path: ' + path

        return getFileSystem(uri, true).getPath(path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        log.info 'Inside newByteChannel() from FileSystemProvider'

        return null
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        log.info 'Inside newInputStream() from FileSystemProvider'
        log.info 'from FileSystemProvider -> newInputStream() -> Path: ' + path.toString()

        if (!(path instanceof SynapsePath)) {
            throw new ProviderMismatchException()
        }

        return ((SynapsePath) path).getFileSystem().newInputStream(path, options, this.authToken)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        log.info 'Inside newDirectoryStream() from FileSystemProvider'

        return null
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        log.info 'Inside createDirectory() from FileSystemProvider'

    }

    @Override
    void delete(Path path) throws IOException {
        log.info 'Inside delete() from FileSystemProvider'

    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        log.info 'Inside copy() from FileSystemProvider'

    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        log.info 'Inside move() from FileSystemProvider'

    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        log.info 'Inside isSameFile() from FileSystemProvider'

        return false
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        log.info 'Inside isHidden() from FileSystemProvider'

        return false
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        log.info 'Inside getFileStore() from FileSystemProvider'

        return null
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        log.info 'Inside checkAccess() from FileSystemProvider'

    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        log.info 'Inside getFileAttributeView() from FileSystemProvider'

        return null
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        log.info 'Inside readAttributes(Path path, Class<A> type, LinkOption... options) from FileSystemProvider'

        return null
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        log.info 'Inside readAttributes(Path path, String attributes, LinkOption... options) from FileSystemProvider'

        return null
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        log.info 'Inside setAttribute() from FileSystemProvider'

    }
}
