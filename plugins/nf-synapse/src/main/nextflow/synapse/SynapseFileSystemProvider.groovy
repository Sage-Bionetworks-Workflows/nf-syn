package nextflow.synapse

import groovy.util.logging.Slf4j

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

@Slf4j
class SynapseFileSystemProvider extends FileSystemProvider {
    @Override
    String getScheme() {
        log.info 'Inside getScheme() from FileSystemProvider'

        return "syn"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        log.info 'Inside newFileSystem() from FileSystemProvider'

        return new SynapseFileSystem()
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        log.info 'Inside getFileSystem() from FileSystemProvider'
        log.info 'URI is ' + uri

        return new SynapseFileSystem()
    }

    @Override
    Path getPath(URI uri) {
        log.info 'Inside getPath() from FileSystemProvider'

        return new SynapsePath()
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        log.info 'Inside newByteChannel() from FileSystemProvider'

        return null
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        log.info 'Inside newInputStream() from FileSystemProvider'

        return new SynapseFileSystem().newInputStream(path, options)
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
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        log.info 'Inside getFileAttributeView() from FileSystemProvider'

        return null
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
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
