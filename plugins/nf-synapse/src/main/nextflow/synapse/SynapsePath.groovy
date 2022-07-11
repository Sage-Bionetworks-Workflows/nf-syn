package nextflow.synapse

import groovy.util.logging.Slf4j

import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

@Slf4j
class SynapsePath implements Path {
    private final byte[] path

    SynapsePath() {
        log.info 'Inside SynapsePath() from Path'

        this.path = '/' as byte[]
    }

    @Override
    FileSystem getFileSystem() {
        log.info 'Inside getFileSystem() from Path'

        return new SynapseFileSystem()
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

        return new SynapsePath()
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

        return null
    }

    @Override
    Path toAbsolutePath() {
        log.info 'Inside toAbsolutePath() from Path'

        return new SynapsePath()
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
