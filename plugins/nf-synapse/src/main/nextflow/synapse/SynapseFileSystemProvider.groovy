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
        log.trace 'Inside getScheme() from FileSystemProvider'

        return "syn"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> config) throws IOException {
        log.trace 'Inside newFileSystem() from FileSystemProvider'

        final authToken = config.get(SYNAPSE_AUTH_TOKEN) as String
        final scheme = uri.scheme.toLowerCase()

        if (scheme != this.getScheme())
            throw new IllegalArgumentException("Not a valid ${getScheme().toUpperCase()} scheme: $scheme")

        final base = uri

        log.trace 'Inside newFileSystem() from FileSystemProvider'

        if (fileSystemMap.containsKey(base))
            throw new IllegalStateException("File system `$base` already exists")

        if(authToken) {
            this.authToken = authToken
        }

        return new SynapseFileSystem(this, base)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        log.trace 'Inside getFileSystem(URI uri) from FileSystemProvider'

        getFileSystem(uri, false)
    }

    FileSystem getFileSystem(URI uri, boolean canCreate) {
        log.trace 'Inside getFileSystem(URI uri, boolean canCreate) from FileSystemProvider'
        log.trace 'getFileSystem() from FileSystemProvider -> URI: ' + uri

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
        log.trace 'Inside getPath() from FileSystemProvider'

        def path = uri.getSchemeSpecificPart()
        path = path.substring(1)

        log.trace 'from FileSystemProvider -> getPath() -> Path: ' + path

        return getFileSystem(uri, true).getPath(path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        log.trace 'Inside newByteChannel() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        log.trace 'Inside newInputStream() from FileSystemProvider'
        log.trace 'from FileSystemProvider -> newInputStream() -> Path: ' + path.toString()

        if (!(path instanceof SynapsePath)) {
            throw new ProviderMismatchException()
        }

        return ((SynapsePath) path).getFileSystem().newInputStream(path, this.authToken)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        log.trace 'Inside newDirectoryStream() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        log.trace 'Inside createDirectory() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void delete(Path path) throws IOException {
        log.trace 'Inside delete() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        log.trace 'Inside copy() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        log.trace 'Inside move() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        log.trace 'Inside isSameFile() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        log.trace 'Inside isHidden() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        log.trace 'Inside getFileStore() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        log.trace 'Inside checkAccess() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        log.trace 'Inside getFileAttributeView() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        log.trace 'Inside readAttributes(Path path, Class<A> type, LinkOption... options) from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        log.trace 'Inside readAttributes(Path path, String attributes, LinkOption... options) from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        log.trace 'Inside setAttribute() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }
}
