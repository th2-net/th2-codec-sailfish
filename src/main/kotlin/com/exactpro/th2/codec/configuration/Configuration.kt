/*
 *  Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.configuration.TransportType.PROTOBUF
import com.exactpro.th2.codec.configuration.TransportType.TH2_TRANSPORT
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.IOException

internal val OBJECT_MAPPER: ObjectMapper = ObjectMapper(JsonFactory()).apply { registerKotlinModule() }
    .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)

class Configuration(
    val defaultSettingResourceName: String = "codec_config.yml",
    val outgoingEventBatchBuildTime: Long = 30,
    val maxOutgoingEventBatchSize: Int = 99,
    val numOfEventBatchCollectorWorkers: Int = 1,
    @Deprecated(
        "Please, use ConverterParameters.allowUnknownEnumValues instead",
        ReplaceWith(
            "converterParameters.allowUnknownEnumValues",
            imports = ["com.exactpro.th2.codec.configuration.ConverterParameters"]
        )
    )
    val allowUnknownEnumValues: Boolean = false,
    val converterParameters: ConverterParameters = ConverterParameters(allowUnknownEnumValues = allowUnknownEnumValues),
    val enabledExternalQueueRouting: Boolean = false,
    var dictionaries: Map<String, String>? = null,
    val enableVerticalScaling: Boolean = false
) {
    var codecClassName: String? = null

    var codecParameters: Map<String, String>? = null

    var transportLines: Map<String, TransportType> = mapOf(
        "" to PROTOBUF,
        "general" to PROTOBUF,
        "transport" to TH2_TRANSPORT,
        "general_transport" to TH2_TRANSPORT
    )

    companion object {

        fun create(commonFactory: CommonFactory): Configuration {
            val configuration = commonFactory.getCustomConfiguration(Configuration::class.java)
            val implementationParams = readSailfishParameters(configuration.defaultSettingResourceName)
            configuration.codecParameters = implementationParams.merge(configuration.codecParameters)
            return configuration
        }


        private fun readSailfishParameters(defaultSettingResourceName: String?): Map<String, String> {
            if (defaultSettingResourceName.isNullOrBlank()) {
                return mapOf()
            }
            return Thread.currentThread().contextClassLoader.getResourceAsStream(defaultSettingResourceName)?.use {
                runCatching {
                    OBJECT_MAPPER.readValue(it, object : TypeReference<LinkedHashMap<String, String>>() {})
                }.getOrElse { e -> when (e) {
                        is IOException -> {
                            throw ConfigurationException("Could not parse '$defaultSettingResourceName' resource file", e)
                        }
                        else -> throw e
                    }
                }
            } ?: mapOf()
        }

        private fun <K, V> Map<K, V>.merge(userParameters: Map<K, V>?): Map<K, V> {
            return if (userParameters == null) {
                this
            } else {
                toMutableMap().apply { putAll(userParameters) }
            }
        }
    }
}

class ConverterParameters(
    val stripTrailingZeros: Boolean = false,
    val allowUnknownEnumValues: Boolean = false
)

enum class TransportType {
    PROTOBUF,
    TH2_TRANSPORT
}



