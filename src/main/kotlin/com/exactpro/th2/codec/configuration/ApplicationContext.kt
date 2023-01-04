/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.DictionaryType.MAIN
import com.exactpro.sf.externalapi.DictionaryType.OUTGOING
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.DefaultMessageFactoryProxy
import com.exactpro.th2.codec.EventBatchCollector
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter.createParameters
import java.io.File
import java.net.URLClassLoader
import java.util.ServiceLoader
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.BooleanUtils.toBoolean
import org.apache.commons.lang3.math.NumberUtils.toByte
import org.apache.commons.lang3.math.NumberUtils.toDouble
import org.apache.commons.lang3.math.NumberUtils.toFloat
import org.apache.commons.lang3.math.NumberUtils.toInt
import org.apache.commons.lang3.math.NumberUtils.toLong
import org.apache.commons.lang3.math.NumberUtils.toShort

class ApplicationContext(
    val commonFactory: CommonFactory,
    val codec: IExternalCodec,
    val codecFactory: IExternalCodecFactory,
    val codecSettings: IExternalCodecSettings,
    val protoToIMessageConverter: ProtoToIMessageConverter,
    val messageToProtoConverter: IMessageToProtoConverter,
    val eventBatchCollector: EventBatchCollector
) {

    companion object {
        private const val CODEC_IMPLEMENTATION_PATH = "codec_implementation"

        private val logger = KotlinLogging.logger { }

        fun create(configuration: Configuration, commonFactory: CommonFactory): ApplicationContext {
            val codecFactory = loadFactory(configuration.codecClassName)

            val eventBatchRouter = commonFactory.eventBatchRouter
            val maxEventBatchSizeInBytes = commonFactory.cradleConfiguration.cradleMaxEventBatchSize
            check(configuration.outgoingEventBatchBuildTime > 0) { "The value of outgoingEventBatchBuildTime must be greater than zero" }
            check(configuration.maxOutgoingEventBatchSize > 0) { "The value of maxOutgoingEventBatchSize must be greater than zero" }
            check(configuration.numOfEventBatchCollectorWorkers > 0) { "The value of numOfEventBatchCollectorWorkers must be greater than zero" }
            val eventBatchCollector = EventBatchCollector(
                eventBatchRouter,
                maxEventBatchSizeInBytes,
                configuration.maxOutgoingEventBatchSize,
                configuration.outgoingEventBatchBuildTime,
                configuration.numOfEventBatchCollectorWorkers,
                commonFactory.boxConfiguration.bookName
            ).apply {
                val protocolName = codecFactory.protocolName
                initEventStructure(commonFactory.boxConfiguration?.boxName ?: protocolName, protocolName, configuration.codecParameters)
            }

            try {
                val codecSettings =
                    createSettings(commonFactory, codecFactory, configuration.codecParameters, configuration)
                val codec = codecFactory.createCodec(codecSettings)
                val dictionaryType = if (OUTGOING in codecSettings.dictionaryTypes) OUTGOING else MAIN
                val dictionary =
                    checkNotNull(codecSettings[dictionaryType]) { "Dictionary is not set: $dictionaryType" }
                val converterParameters = configuration.converterParameters
                val protoConverter = ProtoToIMessageConverter(
                    DefaultMessageFactoryProxy(), dictionary, SailfishURI.unsafeParse(dictionary.namespace),
                    converterParameters.toEncodeParameters()
                )
                return ApplicationContext(
                    commonFactory,
                    codec,
                    codecFactory,
                    codecSettings,
                    protoConverter,
                    IMessageToProtoConverter(converterParameters.toDecodeParameters()),
                    eventBatchCollector
                )
            } catch (e: RuntimeException) {
                eventBatchCollector.createAndStoreErrorEvent(
                    "Codec was not initialized",
                    "An error occurred while initializing the codec",
                    e
                )
                throw e
            }
        }

        private fun ConverterParameters.toEncodeParameters(): ProtoToIMessageConverter.Parameters =
            createParameters().setAllowUnknownEnumValues(allowUnknownEnumValues)

        private fun ConverterParameters.toDecodeParameters(): IMessageToProtoConverter.Parameters =
            IMessageToProtoConverter.parametersBuilder()
                .setStripTrailingZeros(stripTrailingZeros)
                .build()

        private fun createSettings(
            commonFactory: CommonFactory,
            codecFactory: IExternalCodecFactory,
            codecParameters: Map<String, String>?,
            configuration: Configuration
        ): IExternalCodecSettings {
            val settings = codecFactory.createSettings()

            if (codecParameters != null) {
                for ((key, value) in codecParameters) {
                    convertAndSet(settings, key, value)
                }
            }

            val dictionariesFromConfig = configuration.dictionaries
            if (!dictionariesFromConfig.isNullOrEmpty()) {
                loadDictionariesByAliases(dictionariesFromConfig, commonFactory, settings)
            } else {
                loadDictionariesByType(settings, commonFactory)
            }

            return settings
        }

        private fun loadDictionariesByType(
            settings: IExternalCodecSettings,
            commonFactory: CommonFactory
        ) {
            logger.debug { "Loading dictionaries by type" }
            settings.dictionaryTypes.forEach { type ->
                commonFactory.readDictionary(DictionaryType.valueOf(type.name)).use { stream ->
                    settings[type] = XmlDictionaryStructureLoader().load(stream)
                }
            }
        }

        private fun loadDictionariesByAliases(
            dictionariesFromConfig: Map<String, String>,
            commonFactory: CommonFactory,
            settings: IExternalCodecSettings
        ) {
            logger.debug { "Loading dictionaries by aliases" }
            settings.dictionaryTypes.forEach { dictionaryTypeFromSettings ->
                val dictionaryTypeFromConfig = dictionariesFromConfig.entries.find {
                    it.key.equals(dictionaryTypeFromSettings.toString(), true)
                }

                if (dictionaryTypeFromConfig != null) {
                    val alias = dictionaryTypeFromConfig.value
                    commonFactory.loadDictionary(alias).use { stream ->
                        settings[dictionaryTypeFromSettings] = XmlDictionaryStructureLoader().load(stream)
                    }
                } else {
                    val foundedTypes = dictionariesFromConfig.entries.joinToString(", ", "[", "]") {
                        "'${it.key}' with alias '${it.value}'"
                    }
                    val expectedTypes = settings.dictionaryTypes.joinToString(prefix = "[", postfix = "]")
                    logger.error {
                        "Dictionary with type $dictionaryTypeFromSettings not found. " +
                                "Expected types: $expectedTypes. Found: $foundedTypes"
                    }
                    throw IllegalArgumentException("Dictionary type $dictionaryTypeFromSettings can't be loaded")
                }
            }
        }

        private fun convertAndSet(settings: IExternalCodecSettings, propertyName: String, propertyValue: String) {
            val clazz = settings.propertyTypes[propertyName]
            if (clazz == null) {
                logger.warn { "unknown codec parameter '$propertyName'" }
            } else {
                @Suppress("IMPLICIT_CAST_TO_ANY")
                settings[propertyName] = when (clazz) {
                    Boolean::class.javaPrimitiveType,
                    Boolean::class.javaObjectType -> toBoolean(propertyValue)
                    Byte::class.javaPrimitiveType,
                    Byte::class.javaObjectType -> toByte(propertyValue)
                    Short::class.javaPrimitiveType,
                    Short::class.javaObjectType -> toShort(propertyValue)
                    Integer::class.javaPrimitiveType,
                    Integer::class.javaObjectType -> toInt(propertyValue)
                    Long::class.javaPrimitiveType,
                    Long::class.javaObjectType -> toLong(propertyValue)
                    Float::class.javaPrimitiveType,
                    Float::class.javaObjectType -> toFloat(propertyValue)
                    Double::class.javaPrimitiveType,
                    Double::class.javaObjectType -> toDouble(propertyValue)
                    String::class.javaObjectType -> propertyValue
                    SailfishURI::class.java -> SailfishURI.unsafeParse(propertyValue)
                    else -> throw IllegalArgumentException("unsupported class '${clazz.name}' for '$propertyName' codec parameter")
                }.also {
                    logger.info { "codec setting '$propertyName' overridden to '$it'" }
                }
            }
        }

        private fun loadFactory(className: String?): IExternalCodecFactory {
            val jarList = FileUtils.listFiles(
                File(CODEC_IMPLEMENTATION_PATH),
                arrayOf("jar"),
                true
            ).map { it.toURI().toURL() }.toTypedArray()
            val codecClassLoader = URLClassLoader(jarList, ApplicationContext::class.java.classLoader)
            val serviceLoader = ServiceLoader.load(IExternalCodecFactory::class.java, codecClassLoader)
            return serviceLoader.firstOrNull { className == it.javaClass.name }
                ?: throw IllegalArgumentException(
                    "no implementations of $className " +
                            "found by '$CODEC_IMPLEMENTATION_PATH' path"
                )
        }

    }
}