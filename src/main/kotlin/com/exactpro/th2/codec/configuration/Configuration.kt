/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec.configuration

import com.exactpro.th2.configuration.RabbitMQConfiguration
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.io.IOException
import java.lang.System.getenv
import java.nio.file.Files
import java.nio.file.Files.newInputStream
import java.nio.file.Path
import java.nio.file.Paths

data class Configuration(
        var eventStore: EventStoreParameters,
        var codecClassName: String,
        @JsonIgnore
        var codecParameters: Map<String, String>? = null,
        var rabbitMQ: RabbitMQConfiguration,
        var dictionary: String,
        var encoder: CodecParameters? = null,
        var decoder: CodecParameters? = null
) {

    val logger = KotlinLogging.logger { }

    companion object {
        private val objectMapper = ObjectMapper(YAMLFactory()).apply { registerModule(KotlinModule()) }

        private const val EVENT_STORE_HOST = "EVENT_STORE_HOST"
        private const val EVENT_STORE_PORT = "EVENT_STORE_PORT"
        private const val CODEC_CLASS_NAME = "CODEC_CLASS_NAME"
        private const val CODEC_DICTIONARY = "CODEC_DICTIONARY"
        private const val DECODER_PARAMETERS = "DECODER_PARAMETERS"
        private const val ENCODER_PARAMETERS = "ENCODER_PARAMETERS"

        fun create(configPath: String?, sailfishCodecParamsPath: String?): Configuration {
            val configuration = if (configPath == null || !Files.exists(Paths.get(configPath))) {
                createFromEnvVariables()
            } else {
                parse(Paths.get(configPath))
            }
            if (configuration.decoder == null && configuration.encoder == null) {
                throw ConfigurationException(
                        "config file has neither 'encoder' nor 'decoder' elements. " +
                                "Must be present at least one of them."
                )
            }
            configuration.codecParameters = readSailfishParameters(sailfishCodecParamsPath)
            return configuration
        }

        private fun createFromEnvVariables(): Configuration {
            val objectMapper = ObjectMapper().apply { registerModule(KotlinModule()) }
            return Configuration(
                    eventStore = EventStoreParameters(
                            getEnvOrException(EVENT_STORE_HOST),
                            Integer.parseInt(getEnvOrException(EVENT_STORE_PORT))
                    ),
                    codecClassName = getEnvOrException(CODEC_CLASS_NAME),
                    rabbitMQ = RabbitMQConfiguration(),
                    dictionary = getEnvOrException(CODEC_DICTIONARY),
                    encoder = getenv(ENCODER_PARAMETERS)?.let {
                        parseJsonValue(
                                objectMapper,
                                ENCODER_PARAMETERS,
                                it,
                                object : TypeReference<CodecParameters>() {}
                        )
                    },
                    decoder = getenv(DECODER_PARAMETERS)?.let {
                        parseJsonValue(
                                objectMapper,
                                DECODER_PARAMETERS,
                                it,
                                object : TypeReference<CodecParameters>() {}
                        )
                    }
            )
        }

        private fun parse(file: Path): Configuration {
            try {
                return objectMapper.readValue(newInputStream(file), Configuration::class.java)
            } catch (exception: Exception) {
                when (exception) {
                    is IOException,
                    is JsonParseException,
                    is JsonMappingException -> {
                        throw ConfigurationException("could not parse config $file", exception)
                    }
                    else -> throw exception
                }
            }
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

        private fun getEnvOrException(variableName: String): String {
            return getenv(variableName) ?: throw IllegalArgumentException("'$variableName' env variable is not set")
        }

        private fun getEnvOrDefault(variableName: String, defaultValue: String): String {
            return getenv(variableName) ?: defaultValue
        }

        private fun <T> parseJsonValue(
                objectMapper: ObjectMapper,
                variableName: String,
                value: String,
                targetType: TypeReference<T>
        ): T {
            try {
                return objectMapper.readValue(value, targetType)
            } catch (exception: Exception) {
                when (exception) {
                    is IOException,
                    is JsonParseException,
                    is JsonMappingException -> {
                        throw ConfigurationException("could not parse '$variableName' env variable " +
                                "with '$value'", exception)
                    }
                    else -> throw exception
                }
            }
        }
    }
}

data class EventStoreParameters(
        var host: String,
        var port: Int
)

data class CodecParameters(
        @JsonProperty("in") var inParams: InputParameters,
        @JsonProperty("out") var outParams: OutputParameters
)

data class InputParameters(
        var exchangeName: String,
        var queueName: String
)

data class OutputParameters(
        var filters: List<FilterParameters>
)

data class FilterParameters(
        var exchangeName: String,
        var queueName: String,
        var filterType: String,
        var parameters: Map<String, String>? = emptyMap()
)
