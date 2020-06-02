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
import com.rabbitmq.client.ConnectionFactory
import org.apache.commons.io.FileUtils
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

        fun create(configuration: Configuration): ApplicationContext {
            val codecFactory =
                loadFactory(
                    configuration.codecClassName
                )
            val dictionary =
                loadDictionary(
                    configuration.dictionary
                )
            val codecSettings = codecFactory.createSettings(dictionary)
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

        private fun createConnectionFactory(configuration: Configuration): ConnectionFactory {
            val connectionFactory = ConnectionFactory()
            connectionFactory.host = configuration.rabbitMQ.host
            connectionFactory.virtualHost = configuration.rabbitMQ.vHost
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