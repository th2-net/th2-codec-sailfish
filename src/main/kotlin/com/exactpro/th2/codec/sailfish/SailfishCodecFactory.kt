/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec.sailfish

import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.DictionaryType.MAIN
import com.exactpro.sf.externalapi.DictionaryType.OUTGOING
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.CodecException
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.configuration.ConfigurationException
import com.exactpro.th2.codec.sailfish.configuration.SailfishConfiguration
import com.exactpro.th2.codec.sailfish.configuration.ConverterParameters
import com.exactpro.th2.codec.sailfish.proto.ProtoDecodeProcessor
import com.exactpro.th2.codec.sailfish.proto.ProtoDecoder
import com.exactpro.th2.codec.sailfish.proto.ProtoEncodeProcessor
import com.exactpro.th2.codec.sailfish.proto.ProtoEncoder
import com.exactpro.th2.codec.sailfish.transport.TransportDecodeProcessor
import com.exactpro.th2.codec.sailfish.transport.TransportDecoder
import com.exactpro.th2.codec.sailfish.transport.TransportEncodeProcessor
import com.exactpro.th2.codec.sailfish.transport.TransportEncoder
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.sailfish.utils.FromSailfishParameters
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.exactpro.th2.sailfish.utils.ToSailfishParameters
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter
import com.fasterxml.jackson.core.type.TypeReference
import com.google.auto.service.AutoService
import mu.KotlinLogging
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.math.NumberUtils
import java.io.IOException
import java.util.ServiceLoader

@AutoService(IPipelineCodecFactory::class)
class SailfishCodecFactory : IPipelineCodecFactory {
    private lateinit var context: IPipelineCodecContext

    lateinit var codecSettings: IExternalCodecSettings

    override val settingsClass: Class<out SailfishConfiguration>
        get() = SailfishConfiguration::class.java

    override lateinit var protocols: Set<String>

    override fun init(pipelineCodecContext: IPipelineCodecContext) {
        context = pipelineCodecContext
    }

    override fun create(settings: IPipelineCodecSettings?): IPipelineCodec {
        require(settings is SailfishConfiguration) {
            "Unexpected setting type: ${settings?.javaClass ?: "null"}"
        }
        val codecFactory: IExternalCodecFactory = runCatching {
            load<IExternalCodecFactory>(settings.codecClassName)
        }.getOrElse {
            throw IllegalStateException("Failed to load codec factory", it)
        }

        protocols = setOf(codecFactory.protocolName)

        try {
            if (!this::codecSettings.isInitialized) {
                codecSettings = codecFactory.createSetting(
                    context,
                    settings.defaultSettingResourceName,
                    settings.codecParameters,
                    settings.dictionaries
                )
            }

            requireNotNull(codecFactory.createCodec(codecSettings)) {
                "Codec can't be created using ${codecFactory::class.java} factory"
            }
            val dictionaryType = if (OUTGOING in codecSettings.dictionaryTypes) OUTGOING else MAIN
            val dictionary: IDictionaryStructure = checkNotNull(codecSettings[dictionaryType]) {
                "Dictionary is not set: $dictionaryType"
            }

            return with(settings.converterParameters) {
                val protoToIMessageConverter = ProtoToIMessageConverter(dictionary, toEncodeParameters())
                val messageToProtoConverter = IMessageToProtoConverter(toDecodeParameters())
                val transportToIMessageConverter = TransportToIMessageConverter(
                    dictionary = dictionary,
                    parameters = toTransportEncodeParameters()
                )
                val messageToTransportConverter = IMessageToTransportConverter(toTransportDecodeParameters())

                SailfishCodec(
                    ProtoDecoder(ProtoDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter)),
                    ProtoEncoder(ProtoEncodeProcessor(codecFactory, codecSettings, protoToIMessageConverter)),
                    TransportDecoder(
                        TransportDecodeProcessor(
                            codecFactory,
                            codecSettings,
                            messageToTransportConverter
                        )
                    ),
                    TransportEncoder(
                        TransportEncodeProcessor(
                            codecFactory,
                            codecSettings,
                            transportToIMessageConverter
                        )
                    )
                )
            }
        } catch (e: RuntimeException) {
            throw CodecException("Codec was not initialized", e)
        }
    }

    override fun close() {}

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun IExternalCodecFactory.createSetting(
            context: IPipelineCodecContext,
            defaultSettingResourceName: String,
            parameters: Map<String, String>,
            dictionaries: Map<String, String>
        ): IExternalCodecSettings {
            return createSettings().apply {
                readSailfishParameters(defaultSettingResourceName).apply {
                    putAll(parameters)
                }.forEach { (name, value) ->
                    convertAndSet(name, value)
                }
                LOGGER.info { "Overridden ${parameters.keys} codec settings" }

                loadDictionaries(context, dictionaries)
            }
        }

        private fun readSailfishParameters(defaultSettingResourceName: String?): MutableMap<String, String> {
            if (defaultSettingResourceName.isNullOrBlank()) {
                return hashMapOf()
            }
            return Thread.currentThread().contextClassLoader.getResourceAsStream(defaultSettingResourceName)?.use {
                runCatching {
                    MAPPER.readValue(it, object : TypeReference<LinkedHashMap<String, String>>() {})
                }.getOrElse { e ->
                    when (e) {
                        is IOException -> {
                            throw ConfigurationException(
                                "Could not parse '$defaultSettingResourceName' resource file",
                                e
                            )
                        }

                        else -> throw e
                    }
                }
            } ?: hashMapOf()
        }

        private fun IExternalCodecSettings.loadDictionaries(
            context: IPipelineCodecContext,
            dictionariesFromConfig: Map<String, String>
        ) {
            LOGGER.debug { "Loading dictionaries by aliases" }
            dictionaryTypes.forEach { dictionaryTypeFromSettings ->
                val dictionaryTypeFromConfig = dictionariesFromConfig.entries.find {
                    it.key.equals(dictionaryTypeFromSettings.toString(), true)
                }

                if (dictionaryTypeFromConfig != null) {
                    val alias = dictionaryTypeFromConfig.value
                    context[alias].use { stream ->
                        this[dictionaryTypeFromSettings] = XmlDictionaryStructureLoader().load(stream)
                    }
                } else {
                    val foundedTypes = dictionariesFromConfig.entries.joinToString(", ", "[", "]") {
                        "'${it.key}' with alias '${it.value}'"
                    }
                    val expectedTypes = dictionaryTypes.joinToString(prefix = "[", postfix = "]")
                    LOGGER.error {
                        "Dictionary with type $dictionaryTypeFromSettings not found. " +
                                "Expected types: $expectedTypes. Found: $foundedTypes"
                    }
                    throw IllegalArgumentException("Dictionary type $dictionaryTypeFromSettings can't be loaded")
                }
            }
        }

        private fun IExternalCodecSettings.convertAndSet(name: String, value: String) {
            val clazz = propertyTypes[name]
            if (clazz == null) {
                LOGGER.warn { "unknown codec parameter '$name'" }
            } else {
                this[name] = when (clazz) {
                    Boolean::class.javaPrimitiveType,
                    Boolean::class.javaObjectType -> BooleanUtils.toBoolean(value)

                    Byte::class.javaPrimitiveType,
                    Byte::class.javaObjectType -> NumberUtils.toByte(value)

                    Short::class.javaPrimitiveType,
                    Short::class.javaObjectType -> NumberUtils.toShort(value)

                    Integer::class.javaPrimitiveType,
                    Integer::class.javaObjectType -> NumberUtils.toInt(value)

                    Long::class.javaPrimitiveType,
                    Long::class.javaObjectType -> NumberUtils.toLong(value)

                    Float::class.javaPrimitiveType,
                    Float::class.javaObjectType -> NumberUtils.toFloat(value)

                    Double::class.javaPrimitiveType,
                    Double::class.javaObjectType -> NumberUtils.toDouble(value)

                    String::class.javaObjectType -> value
                    SailfishURI::class.java -> SailfishURI.unsafeParse(value)
                    else -> throw IllegalArgumentException("unsupported class '${clazz.name}' for '$name' codec parameter")
                }
            }
        }

        private inline fun <reified T : Any> load(codecClassName: String?): T {
            val instances: List<T> = ServiceLoader.load(T::class.java).toList()

            return when (instances.size) {
                0 -> error("No instances of ${T::class.simpleName}")
                1 -> instances.single()
                else -> codecClassName?.let { className ->
                    instances.find {
                        it::class.java.canonicalName == className
                    } ?: error("found ${instances.size} codec implementation(s) but none matches $className. " +
                            "Implementations: ${instances.joinToString { it::class.java.canonicalName }}"
                    )
                }
                    ?: error("found ${instances.size} codec implementation(s) but no 'codecClassName' parameter was provided")
            }
        }

        private fun ConverterParameters.toTransportEncodeParameters(): ToSailfishParameters =
            ToSailfishParameters(allowUnknownEnumValues = allowUnknownEnumValues)

        private fun ConverterParameters.toTransportDecodeParameters(): FromSailfishParameters =
            FromSailfishParameters(stripTrailingZeros = stripTrailingZeros)

        private fun ConverterParameters.toEncodeParameters(): ProtoToIMessageConverter.Parameters =
            ProtoToIMessageConverter.createParameters().setAllowUnknownEnumValues(allowUnknownEnumValues)

        private fun ConverterParameters.toDecodeParameters(): IMessageToProtoConverter.Parameters =
            IMessageToProtoConverter.parametersBuilder()
                .setStripTrailingZeros(stripTrailingZeros)
                .build()
    }
}