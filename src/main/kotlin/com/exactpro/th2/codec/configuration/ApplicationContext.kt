package com.exactpro.th2.codec.configuration

import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.common.util.EPSCommonException
import com.exactpro.sf.comparison.conversion.MultiConverter
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.IMessageToProtoConverter
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.codec.DefaultMessageFactoryProxy
import com.rabbitmq.client.ConnectionFactory
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.BooleanUtils.toBoolean
import org.apache.commons.lang3.math.NumberUtils
import org.apache.commons.lang3.math.NumberUtils.*
import java.io.File
import java.io.IOException
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

class ApplicationContext(
    val codec: IExternalCodec,
    val codecSettings: IExternalCodecSettings,
    val dictionary: IDictionaryStructure,
    val protoToIMessageConverter: ProtoToIMessageConverter,
    val messageToProtoConverter: IMessageToProtoConverter,
    val connectionFactory: ConnectionFactory
) {

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
                codecSettings,
                dictionary,
                protoConverter,
                iMessageConverter,
                connectionFactory
            )
        }

        private fun createSettings(
            codecFactory: IExternalCodecFactory,
            dictionary: IDictionaryStructure,
            codecParameters: Map<String, String>?
        ): IExternalCodecSettings {
            val settings = codecFactory.createSettings(dictionary)
            if (codecParameters != null) {
                for ((key, value) in codecParameters) {
                    convertAndSet(settings, key, value)
                }
            }
            return settings
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

        fun loadFactory(className: String): IExternalCodecFactory {
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