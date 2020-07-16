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

