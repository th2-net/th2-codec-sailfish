package com.exactpro.th2.codec

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.nio.file.Paths
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configPath: String by option(help = "Path to configuration file").required()
    override fun run() = runBlocking {
        val configuration = Configuration.parse(Paths.get(configPath))
        logger.debug { "Configuration: $configuration" }
        val applicationContext = ApplicationContext.create(configuration)
        val decoder = Decoder(configuration, applicationContext)
        decoder.start(configuration.rabbitMQ)
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() = runBlocking {
                println("Gracefully shutting down")
                decoder.close()
                println("Gracefully shut!!!")
            }
        })
    }
}
