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

import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.common.util.EPSCommonException
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.IMessageToProtoConverter
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.codec.DefaultMessageFactoryProxy
import com.exactpro.th2.eventstore.grpc.AsyncEventStoreServiceService
import com.exactpro.th2.schema.factory.CommonFactory
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.BooleanUtils.toBoolean
import org.apache.commons.lang3.math.NumberUtils.*
import java.io.File
import java.io.IOException
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.streams.asSequence

class ApplicationContext(
    val commonFactory: CommonFactory,
    val codec: IExternalCodec,
    val codecFactory: IExternalCodecFactory,
    val codecSettings: IExternalCodecSettings,
    val dictionary: IDictionaryStructure,
    val protoToIMessageConverter: ProtoToIMessageConverter,
    val messageToProtoConverter: IMessageToProtoConverter,
    val eventStoreService: AsyncEventStoreServiceService?
) {

    companion object {
        private const val CODEC_IMPLEMENTATION_PATH = "codec_implementation"
        private const val DICTIONARIES_PATH = "dictionaries"

        private val logger = KotlinLogging.logger { }

        fun create(configuration: Configuration, commonFactory: CommonFactory): ApplicationContext {
            val codecFactory = loadFactory(configuration.codecClassName)
            val dictionary = loadDictionary(configuration.dictionary)

            val commonFactory = commonFactory
            val eventStoreService = commonFactory.grpcRouter.getService(AsyncEventStoreServiceService::class.java)

            val codecSettings = createSettings(codecFactory, dictionary, configuration.codecParameters)
            val codec = codecFactory.createCodec(codecSettings)
            val protoConverter = ProtoToIMessageConverter(
                DefaultMessageFactoryProxy(), dictionary, SailfishURI.unsafeParse(dictionary.namespace)
            )
            val iMessageConverter = IMessageToProtoConverter()


            return ApplicationContext(
                commonFactory,
                codec,
                codecFactory,
                codecSettings,
                dictionary,
                protoConverter,
                iMessageConverter,
                eventStoreService
            )
        }

        private fun createSettings(
            codecFactory: IExternalCodecFactory,
            dictionary: IDictionaryStructure,
            codecParameters: Map<String, String>?
        ): IExternalCodecSettings {
            val settings = codecFactory.createSettings(dictionary)
            setDictionaryFiles(settings)
            if (codecParameters != null) {
                for ((key, value) in codecParameters) {
                    convertAndSet(settings, key, value)
                }
            }
            return settings
        }

        private fun setDictionaryFiles(settings: IExternalCodecSettings) {
            try {
                settings.dictionaryFiles.putAll(Files.walk(Paths.get(DICTIONARIES_PATH)).asSequence()
                    .filter { !Files.isDirectory(it) }
                    .associate { Pair(SailfishURI.unsafeParse(it.fileName.toString()), it.toFile()) })
            } catch (exception: IOException) {
                logger.warn(exception) { "could not add dictionary files to codec settings" }
            }
        }

        private fun convertAndSet(settings: IExternalCodecSettings, propertyName: String, propertyValue: String) {
            val clazz = settings.propertyTypes[propertyName]
            if (clazz == null) {
                logger.warn { "unknown codec parameter '$propertyName'" }
            } else {
                settings[propertyName] = when(clazz) {
                    Boolean::class.javaPrimitiveType,
                    Boolean::class.javaObjectType -> toBoolean(propertyValue)
                    Byte::class.javaPrimitiveType,
                    Byte::class.javaObjectType -> toByte(propertyValue)
                    Short::class.javaPrimitiveType,
                    Short::class.javaObjectType -> toShort(propertyValue)
                    Integer::class.javaPrimitiveType,
                    Integer::class.javaObjectType-> toInt(propertyValue)
                    Long::class.javaPrimitiveType,
                    Long::class.javaObjectType -> toLong(propertyValue)
                    Float::class.javaPrimitiveType,
                    Float::class.javaObjectType -> toFloat(propertyValue)
                    Double::class.javaPrimitiveType,
                    Double::class.javaObjectType -> toDouble(propertyValue)
                    String::class.javaObjectType -> propertyValue
                    else -> throw IllegalArgumentException("unsupported class '${clazz.name}' for '$propertyName' codec parameter")
                }.also {
                    logger.info { "codec setting '$propertyName' overridden to '$it'" }
                }
            }
        }


        private fun loadDictionary(dictionaryPath: String?): IDictionaryStructure {
            try {
                return XmlDictionaryStructureLoader().load(
                    Files.newInputStream(Paths.get(DICTIONARIES_PATH, dictionaryPath))
                )
            } catch (exception: Exception) {
                when (exception) {
                    is IOException,
                    is EPSCommonException ->
                        throw RuntimeException("could not load dictionary by $dictionaryPath path", exception)
                    else -> throw exception
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