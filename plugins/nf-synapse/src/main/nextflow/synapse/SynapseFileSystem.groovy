package nextflow.synapse

import groovy.util.logging.Slf4j

import java.nio.file.*
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

@Slf4j
class SynapseFileSystem extends FileSystem {
    private SynapseFileSystemProvider provider

    private URI base

    SynapseFileSystem(SynapseFileSystemProvider provider, URI base) {
        log.info 'Inside SynapseFileSystem() from FileSystem'
        log.info 'URI base: ' + base

        this.provider = provider
        this.base = base
    }

    @Override
    FileSystemProvider provider() {
        log.info 'Inside provider() from FileSystem'

        return provider
    }

    URI getBaseUri() {
        return base
    }

    @Override
    void close() throws IOException {
        log.info 'Inside close() from FileSystem'

    }

    @Override
    boolean isOpen() {
        log.info 'Inside isOpen() from FileSystem'

        return false
    }

    @Override
    boolean isReadOnly() {
        log.info 'Inside isReadOnly() from FileSystem'

        return false
    }

    @Override
    String getSeparator() {
        log.info 'Inside getSeparator() from FileSystem'

        return null
    }

    @Override
    Iterable<Path> getRootDirectories() {
        log.info 'Inside getRootDirectories() from FileSystem'

        return null
    }

    @Override
    Iterable<FileStore> getFileStores() {
        log.info 'Inside getFileStores() from FileSystem'

        return null
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        log.info 'Inside supportedFileAttributeViews() from FileSystem'

        return null
    }

    @Override
    Path getPath(String path, String... more) {
        log.info 'Inside getPath() from FileSystem'
        log.info 'from FileSystem -> getPath() -> Path: ' + path

        return new SynapsePath(this, path)
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        log.info 'Inside getPathMatcher() from FileSystem'

        return null
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        log.info 'Inside getUserPrincipalLookupService() from FileSystem'

        return null
    }

    @Override
    WatchService newWatchService() throws IOException {
        log.info 'Inside newWatchService() from FileSystem'

        return null
    }

    static InputStream newInputStream(Path path, OpenOption[] options) throws IOException {
        log.info 'Inside newInputStream() from FileSystem'
        log.info 'from FileSystem -> newInputStream() -> Path: ' + path.toString()

        String initialString = "Tung is testing again!"
        InputStream targetStream = new ByteArrayInputStream(initialString.getBytes())
        return targetStream
    }
}
