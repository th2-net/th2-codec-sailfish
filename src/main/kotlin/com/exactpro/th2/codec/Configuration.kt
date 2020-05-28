package com.exactpro.th2.codec

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

data class Configuration (
        var eventStore: EventStoreParameters,
        var codecClassName: String,
        var rabbitMQ: RabbitMQParameters,
        var dictionary: String,
        var encoding: CodecParameters,
        var decoding: CodecParameters
) {

    val logger = KotlinLogging.logger { }

    companion object {
        val objectMapper = ObjectMapper(YAMLFactory()).apply { registerModule(KotlinModule()) }
        val ENCODE_EXCHANGE_NAME = "ENCODE_EXCHANGE_NAME"
        val ENCODE_IN_QUEUE_NAME = "ENCODE_IN_QUEUE_NAME"
        val ENCODE_OUT_FILTERS = "ENCODE_OUT_FILTERS"
        val DECODE_EXCHANGE_NAME = "DECODE_EXCHANGE_NAME"
        val DECODE_IN_QUEUE_NAME = "DECODE_IN_QUEUE_NAME"
        val DECODE_OUT_FILTERS = "DECODE_OUT_FILTERS"

        fun parse(file: Path): Configuration {
            try {
                return objectMapper.readValue(Files.newInputStream(file), Configuration::class.java)
            } catch (exception: Exception) {
                when(exception) {
                    is IOException,
                    is JsonParseException,
                    is JsonMappingException-> {
                        throw RuntimeException("could not parse config $file", exception)
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

data class RabbitMQParameters (
    var host: String,
    var vHost: String,
    var port: Int,
    var username: String,
    var password: String
)

data class CodecParameters (
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

data class FilterParameters (
    var exchangeName: String,
    var queueName: String,
    var filterType: String,
    var parameters: Map<String, String>?
)
