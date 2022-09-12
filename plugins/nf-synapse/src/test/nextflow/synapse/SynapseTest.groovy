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

import org.sagebionetworks.client.exceptions.SynapseUnauthorizedException
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification

import java.nio.charset.Charset
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Unit test cases for Synapse FileSystem
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Requires({System.getenv('SYNAPSE_AUTH_TOKEN')})
class SynapseTest extends Specification {

    // Please create a file name 'TxtTestFile.txt' and upload the file into your Synapse repo
    // The file should have the content as 'This is a test file'
    // Copy & Paste the Syn ID here
    @Shared
    def fileEntityWithContent = 'syn://syn33282971'

    // Please create a dataset in your Synapse repo
    // Copy & Paste the Syn ID here
    @Shared
    def invalidEntity = 'syn://syn35358478'

    def 'should return error missing token' () {
        given:
        def env = [SYNAPSE_AUTH_TOKEN: null]
        def synapseFileProvider = Spy(SynapseFileSystemProvider)

        when:
        synapseFileProvider.newFileSystem(new URI(fileEntityWithContent), env)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return error unauthorized token' () {
        given:
        def env = [SYNAPSE_AUTH_TOKEN: 'unauthorized_token']
        def synapseFileProvider = Spy(SynapseFileSystemProvider)

        when:
        synapseFileProvider.newFileSystem(new URI(fileEntityWithContent), env)

        then:
        thrown(SynapseUnauthorizedException)
    }

    def 'should return Synapse storage scheme' () {
        given:
        def synapseProvider = new SynapseFileSystemProvider()
        expect:
        synapseProvider.getScheme() == 'syn'
    }

    def 'should read a file content' () {
        given:
        def synapseFilePath = Paths.get(new URI(fileEntityWithContent))
        expect:
        Files.readAllLines(synapseFilePath, Charset.forName('UTF-8')).get(0) == 'This is a test file'
    }

    def 'should return error filesystem already exist' () {
        given:
        def env = [SYNAPSE_AUTH_TOKEN: System.getenv('SYNAPSE_AUTH_TOKEN')]
        def synapseFileProvider = Spy(SynapseFileSystemProvider)

        when:
        synapseFileProvider.newFileSystem(new URI(fileEntityWithContent), env)
        synapseFileProvider.newFileSystem(new URI(fileEntityWithContent), env)

        then:
        thrown(FileSystemAlreadyExistsException)
    }

    def 'should create local file' () {
        given:
        def synapseFileInputStream = Files.newInputStream(Paths.get(new URI(fileEntityWithContent)))

        def localTestFile = new File("../../../TxtTestFile.txt")

        if (localTestFile.exists()) {
            localTestFile.delete()
            localTestFile = new File("../../../TxtTestFile.txt")
        }

        Files.copy(synapseFileInputStream, localTestFile.toPath())

        def testFileReader = new BufferedReader(new FileReader(localTestFile));

        def lineCount = 0;
        while((testFileReader.readLine()) != null) {
            lineCount++;
        }

        expect:
        lineCount == 1
    }

    def 'should return error entity type not supported' () {
        when:
        Paths.get(new URI(invalidEntity))

        then:
        thrown(IllegalArgumentException)
    }
}
