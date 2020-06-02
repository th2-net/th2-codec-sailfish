package com.exactpro.th2.codec.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import mu.KotlinLogging
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

data class Configuration(
    var eventStore: EventStoreParameters,
    var codecClassName: String,
    var rabbitMQ: RabbitMQParameters,
    var dictionary: String,
    var encoder: CodecParameters? = null,
    var decoder: CodecParameters? = null
) {

    val logger = KotlinLogging.logger { }

    companion object {
        private val objectMapper = ObjectMapper(YAMLFactory()).apply { registerModule(KotlinModule()) }

        fun parse(file: Path): Configuration {
            try {
                val configuration = objectMapper.readValue(Files.newInputStream(file), Configuration::class.java)
                if (configuration.decoder == null && configuration.encoder == null) {
                    throw ConfigurationException(
                        "config file has neither 'encoder' nor 'decoder' elements. " +
                                "Must be present at least one of them."
                    )
                }
                return configuration
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
    }
}

data class EventStoreParameters(
    var host: String,
    var port: Int
)

data class RabbitMQParameters(
    var host: String,
    var vHost: String,
    var port: Int,
    var username: String,
    var password: String
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
