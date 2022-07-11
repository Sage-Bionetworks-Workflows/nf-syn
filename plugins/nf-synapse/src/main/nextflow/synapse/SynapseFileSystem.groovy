package nextflow.synapse

import groovy.util.logging.Slf4j

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

@Slf4j
class SynapseFileSystem extends FileSystem {
    @Override
    FileSystemProvider provider() {
        log.info 'Inside provider() from FileSystem'

        return new SynapseFileSystemProvider()
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
    Path getPath(String first, String... more) {
        log.info 'Inside getPath() from FileSystem'

        return new SynapsePath()
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

    InputStream newInputStream(Path path, OpenOption[] options) throws IOException {
        log.info 'Inside newInputStream() from FileSystem'

        String initialString = "Tung is testing again!";
        InputStream targetStream = new ByteArrayInputStream(initialString.getBytes());
        return targetStream;
    }
}
