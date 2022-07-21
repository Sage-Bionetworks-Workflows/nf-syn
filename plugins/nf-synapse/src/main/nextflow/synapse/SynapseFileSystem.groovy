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

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import groovy.util.logging.Slf4j
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.utils.URIUtils
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.nio.charset.StandardCharsets
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.OpenOption
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

        String pathString = path.toString().substring(1)
        log.info 'from FileSystem -> newInputStream() -> pathString: ' + pathString

        // Put your Synapse Auth Token here
        String Synapse_Auth_Token = ""

        CloseableHttpClient httpclient = HttpClients.createDefault()
        HttpClientContext context = HttpClientContext.create()

        HttpGet entityIDGet = new HttpGet("https://repo-prod.prod.sagebase.org/repo/v1/entity/" + pathString)
        entityIDGet.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + Synapse_Auth_Token)
        HttpResponse entityIDGetResponse = httpclient.execute(entityIDGet)
        HttpEntity entityIDGetEntity = entityIDGetResponse.getEntity()

        String entityIDGetResStr = EntityUtils.toString(entityIDGetEntity, StandardCharsets.UTF_8);
        JsonObject entityIDGetResObj = new JsonParser().parse(entityIDGetResStr).getAsJsonObject();

        Integer dataFileHandleId = (Integer) entityIDGetResObj.get("dataFileHandleId").getAsBigInteger();

        HttpGet absoluteURLGet = new HttpGet("https://repo-prod.prod.sagebase.org/file/v1/file/" + dataFileHandleId + "?fileAssociateType=FileEntity&fileAssociateId=" + dataFileHandleId)
        absoluteURLGet.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + Synapse_Auth_Token)
        httpclient.execute(absoluteURLGet, context)

        HttpHost target = context.getTargetHost()
        List<URI> redirectLocations = context.getRedirectLocations()
        URI absoluteURL = URIUtils.resolve(absoluteURLGet.getURI(), target, redirectLocations)

        HttpGet fileContentGet = new HttpGet(absoluteURL.toASCIIString())
        HttpResponse fileContentGetResponse = httpclient.execute(fileContentGet)
        HttpEntity fileContentGetEntity = fileContentGetResponse.getEntity()

        InputStream inputStream = fileContentGetEntity.getContent()
        log.info('Stream' + inputStream)

        return inputStream
    }
}
