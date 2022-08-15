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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.util.SerializerRegistrant

/**
 * Implements Serializer for {@link SynapsePath} objects
 *
 * @author Tung Nguyen <tung.nguyen@tungthecoder.dev>
 */
@Slf4j
@CompileStatic
class SynapsePathSerializer extends Serializer<SynapsePath> implements SerializerRegistrant {

    @Override
    void write(Kryo kryo, Output output, SynapsePath path) {
        log.trace "Synapse entity storage path serialisation > path=$path"
        output.writeString(path.toUriString())
    }

    @Override
    SynapsePath read(Kryo kryo, Input input, Class<SynapsePath> type) {
        final path = input.readString()
        log.trace "Synapse entity storage path > path=$path"
        return (SynapsePath) FileHelper.asPath(path)
    }

    @Override
    void register(Map<Class, Object> serializers) {
        serializers.put(SynapsePath, SynapsePathSerializer)
    }
}
