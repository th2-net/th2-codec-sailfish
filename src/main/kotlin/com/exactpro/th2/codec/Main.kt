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

package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configs: String? by option(help = "Directory containing schema files")
    private val sailfishCodecConfig: String? by option(help = "Path to sailfish codec configuration file")

    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

    init {
        Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown") {
            try {
                logger.info { "Shutdown start" }
                resources.descendingIterator().forEach { action ->
                    runCatching(action).onFailure { logger.error(it.message, it) }
                }
            } finally {
                logger.info { "Shutdown end" }
            }
        })
    }

    override fun run() = runBlocking {
        try {
            val commonFactory = (if (configs == null)
                CommonFactory()
            else
                CommonFactory.createFromArguments("--configs=$configs"))
                .also {
                    resources.add {
                        logger.info("Closing common factory")
                        it.close()
                    }
                }

            val configuration = Configuration.create(commonFactory, sailfishCodecConfig)
            val applicationContext = ApplicationContext.create(configuration, commonFactory)
            val groupRouter = applicationContext.commonFactory.messageRouterMessageGroupBatch

            resources.addFirst { applicationContext.eventBatchCollector.close() }


            createAndStartCodec("decoder", applicationContext)
            { _: ApplicationContext ->
                SyncDecoder(
                    groupRouter, applicationContext,
                    DecodeProcessor(
                        applicationContext.codecFactory,
                        applicationContext.codecSettings,
                        applicationContext.messageToProtoConverter,
                        applicationContext.eventBatchCollector
                    )
                ).also { it.start(configuration.decoderInputAttribute, configuration.decoderOutputAttribute) }
            }

            createAndStartCodec("encoder", applicationContext)
            { _: ApplicationContext ->
                SyncEncoder(
                    groupRouter, applicationContext,
                    EncodeProcessor(
                        applicationContext.codecFactory,
                        applicationContext.codecSettings,
                        applicationContext.protoToIMessageConverter,
                        applicationContext.eventBatchCollector
                    )
                ).also { it.start(configuration.encoderInputAttribute, configuration.encoderOutputAttribute) }
            }

            createGeneralDecoder(applicationContext, configuration)
            createGeneralEncoder(applicationContext, configuration)
            logger.info { "codec started" }
        } catch (exception: Exception) {
            logger.error(exception) { "fatal error. Exit the program" }
            exitProcess(1)
        }
    }

    private fun createGeneralEncoder(
        context: ApplicationContext,
        configuration: Configuration
    ) {
        createAndStartCodec("general-encoder", context)
        { _: ApplicationContext ->
            val router = context.commonFactory.messageRouterMessageGroupBatch
            SyncEncoder(
                router, context,
                EncodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.protoToIMessageConverter,
                    context.eventBatchCollector
                )
            ).also { it.start(configuration.generalEncoderInputAttribute, configuration.generalEncoderOutputAttribute) }
        }
    }

    private fun createGeneralDecoder(
        context: ApplicationContext,
        configuration: Configuration
    ) {
        createAndStartCodec("general-decoder", context)
        { _: ApplicationContext ->
            val router = context.commonFactory.messageRouterMessageGroupBatch
            SyncDecoder(
                router, context,
                DecodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.messageToProtoConverter,
                    context.eventBatchCollector
                )
            ).also { it.start(configuration.generalDecoderInputAttribute, configuration.generalDecoderOutputAttribute) }
        }
    }

    private fun createAndStartCodec(
        codecName: String,
        applicationContext: ApplicationContext,
        creationFunction: (ApplicationContext) -> AutoCloseable
    ) {
        creationFunction.invoke(applicationContext).also {
            resources.add {
                logger.info { "Closing '$codecName' codec" }
                it.close()
            }
        }
        logger.info { "'$codecName' started" }
    }

}



