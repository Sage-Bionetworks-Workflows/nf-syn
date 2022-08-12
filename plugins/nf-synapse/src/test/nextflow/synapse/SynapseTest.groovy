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

import nextflow.file.FileHelper
import spock.lang.Requires
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths

/**
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Requires({System.getenv('SYNAPSE_AUTH_TOKEN')})
class SynapseTest extends Specification {

    def testSynapseFile() {
        def props = new HashMap<>();
        props.put(SynapseFileSystemProvider.SYNAPSE_AUTH_TOKEN, System.getenv('SYNAPSE_AUTH_TOKEN'))

        def synapseFileProvider = FileHelper.getOrCreateFileSystemFor(new URI("syn://syn32193541"), props).provider()
        def synapseFileInputStream = synapseFileProvider.newInputStream(Paths.get(new URI("syn://syn32193541")))

        def synapseTestFile = new File("../../../SynapseTestPDFFile.pdf")

        if (synapseTestFile.exists()) {
            synapseTestFile.delete()
            synapseTestFile = new File("../../../SynapseTestPDFFile.pdf")
        }

        Files.copy(synapseFileInputStream, synapseTestFile.toPath())

        def testFileReader = new BufferedReader(new FileReader(synapseTestFile));

        def lineCount = 0;
        while((testFileReader.readLine()) != null) {
            lineCount++;
        }

        expect:
            lineCount == 3075
    }

}
