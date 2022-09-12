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

import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import org.sagebionetworks.client.SynapseClientImpl

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystemAlreadyExistsException
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
    private Map<URI, SynapseFileSystem> fileSystemMap = [:]
    public static final String SYNAPSE_AUTH_TOKEN = 'SYNAPSE_AUTH_TOKEN'
    private Map<String,String> env = new HashMap<>(System.getenv())
    private String authToken = null

    @Override
    String getScheme() {
        log.trace 'Inside getScheme() from FileSystemProvider'

        return "syn"
    }

    @Memoized
    protected static SynapseClientImpl createSynapseClientWithToken(String authToken) {
        log.trace "Creating Synapse Client with authToken: ${authToken}"

        final synapseClient = new SynapseClientImpl()
        synapseClient.setBearerAuthorizationToken(authToken)

        return synapseClient
    }

    @Override
    SynapseFileSystem newFileSystem(URI uri, Map<String, ?> config) throws IOException {
        log.trace 'Inside newFileSystem() from FileSystemProvider'

        if(fileSystemMap.containsKey(uri))
            throw new FileSystemAlreadyExistsException("File system already exists for entity: `$uri`")

        final authToken = config.get(SYNAPSE_AUTH_TOKEN) as String
        if(!authToken)
            throw new IllegalArgumentException("Missing SYNAPSE_AUTH_TOKEN!")
        this.authToken = authToken

        final synapseClient = createSynapseClientWithToken(authToken)
        final synID = getSynIDFromURI(uri)
        final entity = synapseClient.getEntityById(synID)
        final entityTypeArr = entity.getConcreteType().split("\\.")
        final entityType = entityTypeArr[entityTypeArr.size() - 1]

        if(entityType != 'FileEntity') {
            throw new IllegalArgumentException("Entity type: " + entityType + " is not supported. Only entity type: FileEntity is supported!")
        }

        final result = new SynapseFileSystem(this, uri, synapseClient)
        fileSystemMap[uri] = result

        return result
    }

    static String getSynIDFromURI(URI uri) {
        final synID = uri.getSchemeSpecificPart().substring(2)

        return synID
    }

    @Override
    SynapseFileSystem getFileSystem(URI uri) {
        log.trace 'Inside getFileSystem(URI uri) from FileSystemProvider'

        getFileSystem(uri, false)
    }

    SynapseFileSystem getFileSystem(URI uri, boolean canCreate) {
        log.trace 'Inside getFileSystem(URI uri, boolean canCreate) from FileSystemProvider'

        final scheme = uri.scheme.toLowerCase()

        if (scheme != this.getScheme())
            throw new IllegalArgumentException("Not a valid ${getScheme().toUpperCase()} scheme: $scheme")

        def fs = fileSystemMap.get(uri)

        if(!fs) {
            if(canCreate) {
                fs = newFileSystem(uri, env)
            } else {
                throw new FileSystemNotFoundException("Missing Synapse file system for entity: `$uri`")
            }
        }

        return fs
    }

    @Override
    SynapsePath getPath(URI uri) {
        log.trace 'Inside getPath() from FileSystemProvider'

        def path = uri.getSchemeSpecificPart()
        path = path.substring(1)

        return getFileSystem(uri, true).getPath(path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        log.trace 'Inside newByteChannel() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation newByteChannel() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        log.trace 'Inside newInputStream() from FileSystemProvider'
        log.trace 'from FileSystemProvider -> newInputStream() -> Path: ' + path.toString()

        if (!(path instanceof SynapsePath)) {
            throw new ProviderMismatchException()
        }

        final synapsePath = (SynapsePath) path
        log.trace 'from FileSystemProvider -> newInputStream() -> Path -> after synapsePath: ' + authToken
        log.trace 'from FileSystemProvider -> newInputStream() -> Path -> after synapsePath: ' + synapsePath.synapseClient()

        return synapsePath.getFileSystem().newInputStream(synapsePath)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        log.trace 'Inside newDirectoryStream() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation newDirectoryStream() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        log.trace 'Inside createDirectory() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation createDirectory() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void delete(Path path) throws IOException {
        log.trace 'Inside delete() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation delete() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        log.trace 'Inside copy() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation copy() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        log.trace 'Inside move() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation move() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        log.trace 'Inside isSameFile() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation isSameFile() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        log.trace 'Inside isHidden() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation isHidden() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        log.trace 'Inside getFileStore() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation getFileStore() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        log.trace 'Inside checkAccess() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation checkAccess() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        log.trace 'Inside getFileAttributeView() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation getFileAttributeView() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        log.trace 'Inside readAttributes(Path path, Class<A> type, LinkOption... options) from FileSystemProvider'

        throw new UnsupportedOperationException("Operation readAttributes() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        log.trace 'Inside readAttributes(Path path, String attributes, LinkOption... options) from FileSystemProvider'

        throw new UnsupportedOperationException("Operation readAttributes() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        log.trace 'Inside setAttribute() from FileSystemProvider'

        throw new UnsupportedOperationException("Operation setAttribute() in SynapseFileSystemProvider is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }
}
