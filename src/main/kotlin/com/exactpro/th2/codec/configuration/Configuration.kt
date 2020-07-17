/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.configuration

import com.exactpro.th2.schema.factory.CommonFactory
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.commons.lang3.StringUtils
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Files.newInputStream
import java.nio.file.Paths

class Configuration() {
    var codecClassName: String? = null

    var codecParameters: Map<String, String>? = null
    var dictionary: String? = null

    var decoderInputAttribute: String = "decoder_in"
    var decoderOutputAttribute: String = "decoder_out"

    var encoderInputAttribute: String = "encoder_in"
    var encoderOutputAttribute: String = "encoder_out"

    var generalDecoderInputAttribute: String = "general_decoder_in"
    var generalDecoderOutputAttribute: String = "general_decoder_out"

    var generalEncoderInputAttribute: String = "general_encoder_in"
    var generalEncoderOutputAttribute: String = "general_encoder_out"

    companion object {
        private val objectMapper = ObjectMapper(YAMLFactory()).apply { registerModule(KotlinModule()) }
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)

        fun create(commonFactory : CommonFactory, sailfishCodecParamsPath: String?) : Configuration{

            val configuration = commonFactory.getCustomConfiguration(Configuration::class.java)
            configuration.codecParameters = readSailfishParameters(sailfishCodecParamsPath)
            return configuration
        }


        private fun readSailfishParameters(sailfishCodecParamsPath: String?): Map<String, String> {
            if (StringUtils.isBlank(sailfishCodecParamsPath)) {
                return mapOf()
            }
            val codecParameterFile = Paths.get(sailfishCodecParamsPath)
            if (!Files.exists(codecParameterFile)) {
                return mapOf()
            }
            try {
                return objectMapper.readValue(
                        newInputStream(codecParameterFile),
                        object : TypeReference<LinkedHashMap<String, String>?>() {}
                )
            } catch (exception: Exception) {
                when (exception) {
                    is IOException,
                    is JsonParseException,
                    is JsonMappingException -> {
                        throw ConfigurationException("could not parse '$sailfishCodecParamsPath' file", exception)
                    }
                    else -> throw exception
                }
            }
        }

    }
}



