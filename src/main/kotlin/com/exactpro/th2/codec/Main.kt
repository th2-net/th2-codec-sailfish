package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.CodecParameters
import com.exactpro.th2.codec.configuration.Configuration
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configPath: String? by option(help = "Path to configuration file")
    private val sailfishCodecConfigPath: String? by option(help = "Path to sailfish codec configuration file")
    override fun run() = runBlocking {
        try {
            runProgram(configPath, sailfishCodecConfigPath)
        } catch (exception: Exception) {
            logger.error(exception) { "fatal error. Exit the program" }
        }
    }

    @ObsoleteCoroutinesApi
    private fun runProgram(configPath: String?, sailfishCodecParamsPath: String?) {
        val configuration = Configuration.create(configPath, sailfishCodecParamsPath)
        logger.debug { "Configuration: $configuration" }
        val applicationContext = ApplicationContext.create(configuration)
        createAndStartCodec(
            configuration.decoder,
            "decoder",
            applicationContext
        ) { config: CodecParameters, context: ApplicationContext ->
            SyncDecoder(config, context).also { it.start(configuration.rabbitMQ) }
        }
        createAndStartCodec(
            configuration.encoder,
            "encoder",
            applicationContext
        ) { config: CodecParameters, context: ApplicationContext ->
            Encoder(config, context).also { it.start(configuration.rabbitMQ) }
        }
        logger.info { "codec started" }
    }

    private fun createAndStartCodec(
        codecParameters: CodecParameters?,
        codecName: String,
        applicationContext: ApplicationContext,
        creationFunction: (CodecParameters, ApplicationContext) -> AutoCloseable
    ) {
        if (codecParameters == null) {
            logger.info { "'$codecName' element is not set in the configuration. Skip creating '$codecName'" }
            return
        }
        val codecInstance = creationFunction.invoke(codecParameters, applicationContext)
        logger.info { "'$codecName' started" }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                try {
                    logger.info { "shutting down '$codecName'..." }
                    codecInstance.close()
                    logger.info { "'$codecName' closed successfully" }
                } catch (exception: Exception) {
                    logger.error(exception) { "Error upon '$codecName' closing" }
                }
            }
        })
    }
}

