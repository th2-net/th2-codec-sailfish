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

import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.common.util.EPSCommonException
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.CumulativeDecodeProcessor
import com.exactpro.th2.codec.DefaultMessageFactoryProxy
import com.exactpro.th2.codec.SequentialDecodeProcessor
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.estore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.estore.grpc.EventStoreServiceGrpc.newFutureStub
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.rabbitmq.client.ConnectionFactory
import io.grpc.ManagedChannelBuilder.forAddress
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.BooleanUtils.toBoolean
import org.apache.commons.lang3.RegExUtils
import org.apache.commons.lang3.math.NumberUtils.toByte
import org.apache.commons.lang3.math.NumberUtils.toDouble
import org.apache.commons.lang3.math.NumberUtils.toFloat
import org.apache.commons.lang3.math.NumberUtils.toInt
import org.apache.commons.lang3.math.NumberUtils.toLong
import org.apache.commons.lang3.math.NumberUtils.toShort
import java.io.File
import java.io.IOException
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.ServiceLoader
import kotlin.streams.asSequence

class ApplicationContext(
    val codec: IExternalCodec,
    val codecFactory: IExternalCodecFactory,
    val codecSettings: IExternalCodecSettings,
    val dictionary: IDictionaryStructure,
    val protoToIMessageConverter: ProtoToIMessageConverter,
    val messageToProtoConverter: IMessageToProtoConverter,
    val connectionFactory: ConnectionFactory,
    val eventConnector: EventStoreServiceFutureStub?
) {
    fun createDecodeProcessor(type: ProcessorType): AbstractCodecProcessor<RawMessageBatch, MessageBatch> {
        return when (type) {
            ProcessorType.CUMULATIVE -> CumulativeDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter)
            ProcessorType.SEQUENTIAL -> SequentialDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter)
        }
    }

    companion object {
        private const val CODEC_IMPLEMENTATION_PATH = "codec_implementation"
        private const val DICTIONARIES_PATH = "dictionaries"

        private val logger = KotlinLogging.logger { }

        fun create(configuration: Configuration): ApplicationContext {
            val codecFactory =
                loadFactory(
                    configuration.codecClassName
                )
            val dictionary =
                loadDictionary(
                    configuration.dictionary
                )
            val codecSettings = createSettings(codecFactory, dictionary, configuration.codecParameters)
            val codec = codecFactory.createCodec(codecSettings)
            val protoConverter = ProtoToIMessageConverter(
                DefaultMessageFactoryProxy(), dictionary, SailfishURI.unsafeParse(dictionary.namespace)
            )
            val iMessageConverter = IMessageToProtoConverter()
            val connectionFactory =
                createConnectionFactory(
                    configuration
                )
            return ApplicationContext(
                codec,
                codecFactory,
                codecSettings,
                dictionary,
                protoConverter,
                iMessageConverter,
                connectionFactory,
                createEventStoreConnector(configuration.eventStore)
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
                    .associate { Pair(toSailfishURI(it), it.toFile()) })
            } catch (exception: IOException) {
                logger.warn(exception) { "could not add dictionary files to codec settings" }
            }
        }

        private fun toSailfishURI(it: Path?): SailfishURI {
            val baseName = FilenameUtils.getBaseName(it?.fileName?.toString())
            return SailfishURI.unsafeParse(RegExUtils.replaceAll(baseName, "[.]", "_"))
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
                    SailfishURI::class.java -> SailfishURI.unsafeParse(propertyValue)
                    else -> throw IllegalArgumentException("unsupported class '${clazz.name}' for '$propertyName' codec parameter")
                }.also {
                    logger.info { "codec setting '$propertyName' overridden to '$it'" }
                }
            }
        }

        private fun createConnectionFactory(configuration: Configuration): ConnectionFactory {
            val connectionFactory = ConnectionFactory()
            connectionFactory.host = configuration.rabbitMQ.host
            connectionFactory.virtualHost = configuration.rabbitMQ.virtualHost
            connectionFactory.port = configuration.rabbitMQ.port
            connectionFactory.username = configuration.rabbitMQ.username
            connectionFactory.password = configuration.rabbitMQ.password
            return connectionFactory
        }

        private fun loadDictionary(dictionaryPath: String): IDictionaryStructure {
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

        private fun loadFactory(className: String): IExternalCodecFactory {
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

        private fun createEventStoreConnector(eventStoreParameters: EventStoreParameters): EventStoreServiceFutureStub? {
            return try {
                newFutureStub(forAddress(eventStoreParameters.host, eventStoreParameters.port).usePlaintext().build())
            } catch (exception: Exception) {
                logger.warn(exception) { "could not create event store connector" }
                null
            }
        }
    }
}