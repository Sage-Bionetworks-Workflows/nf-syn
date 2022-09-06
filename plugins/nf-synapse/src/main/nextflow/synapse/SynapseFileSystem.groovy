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
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.sagebionetworks.client.SynapseClientImpl
import org.sagebionetworks.repo.model.file.FileHandleAssociateType
import org.sagebionetworks.repo.model.file.FileHandleAssociation
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

/**
 * Implements NIO File system for Synapse Entity Storage
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
class SynapseFileSystem extends FileSystem {
    private SynapseFileSystemProvider provider

    private URI base

    private SynapseClientImpl synapseClient

    SynapseFileSystem(SynapseFileSystemProvider provider, URI base, SynapseClientImpl synapseClient) {
        log.trace 'Inside SynapseFileSystem() from FileSystem'
        log.trace 'URI base: ' + base

        this.provider = provider
        this.base = base
        this.synapseClient = synapseClient
    }

    @Override
    FileSystemProvider provider() {
        log.trace 'Inside provider() from FileSystem'

        return provider
    }

    URI getBaseUri() {
        log.trace 'Inside getBaseUri() from FileSystem'

        return base
    }

    SynapseClientImpl getSynapseClient() {
        log.trace 'Inside getSynapseClient() from FileSystem'

        return synapseClient
    }

    @Override
    void close() throws IOException {
        log.trace 'Inside close() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isOpen() {
        log.trace 'Inside isOpen() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    boolean isReadOnly() {
        log.trace 'Inside isReadOnly() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    String getSeparator() {
        log.trace 'Inside getSeparator() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Iterable<Path> getRootDirectories() {
        log.trace 'Inside getRootDirectories() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Iterable<FileStore> getFileStores() {
        log.trace 'Inside getFileStores() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        log.trace 'Inside supportedFileAttributeViews() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    SynapsePath getPath(String path, String... more) {
        log.trace 'Inside getPath() from FileSystem'

        return new SynapsePath(this, path)
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        log.trace 'Inside getPathMatcher() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        log.trace 'Inside getUserPrincipalLookupService() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    @Override
    WatchService newWatchService() throws IOException {
        log.trace 'Inside newWatchService() from FileSystem'

        throw new UnsupportedOperationException("Operation is not supported. Please contact nf-synapse plugin admin & raise a ticket in gitHub repo!")
    }

    static InputStream newInputStream(SynapsePath path) throws IOException {
        log.trace 'Inside newInputStream() from FileSystem'
        log.trace 'from FileSystem -> newInputStream() -> Path: ' + path.toString()

        final entityId = path.toString().substring(1)
        final synapseClient = path.synapseClient()

        final entity = synapseClient.getEntityById(entityId)
        final entityTypeArr = entity.getConcreteType().split("\\.")
        final entityType = FileHandleAssociateType.valueOf(entityTypeArr[entityTypeArr.size() - 1])

        final fileHandle = synapseClient.getEntityFileHandlesForCurrentVersion(entityId).getList().get(0)
        final fileHandleAssociation = new FileHandleAssociation()
                                                        .setFileHandleId(fileHandle.getId())
                                                        .setAssociateObjectType(entityType)
                                                        .setAssociateObjectId(fileHandle.getId())

        final downloadURL = synapseClient.getFileURL(fileHandleAssociation)

        final httpclient = HttpClients.createDefault()
        final fileContentGet = new HttpGet(downloadURL.toString())
        final fileContentGetResponse = httpclient.execute(fileContentGet)
        final fileContentGetEntity = fileContentGetResponse.getEntity()

        final inputStream = fileContentGetEntity.getContent()

        return inputStream
    }
}
