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

import com.exactpro.th2.codec.configuration.*
import com.exactpro.th2.eventstore.grpc.StoreEventRequest
import com.exactpro.th2.infra.grpc.Event
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.EventStatus.SUCCESS
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.time.LocalDateTime

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
        val rootEventId = createAndStoreRootEvent(applicationContext)
        if (configuration.decoder == null) {
            logger.info { "'decoder' element is not set in the configuration. Skip creating 'decoder" }
        } else {
            createAndStartCodec(configuration.decoder!!, "decoder", applicationContext, rootEventId)
            { _: CodecParameters, _: ApplicationContext, _: EventID? ->
                SyncDecoder(configuration.decoder!!, applicationContext,
                    DecodeProcessor(
                        applicationContext.codecFactory,
                        applicationContext.codecSettings,
                        applicationContext.messageToProtoConverter
                    ),
                    rootEventId).also { it.start(configuration.rabbitMQ) }
            }
        }
        if (configuration.encoder == null) {
            logger.info { "'encoder' element is not set in the configuration. Skip creating 'encoder" }
        } else {
            createAndStartCodec(configuration.encoder!!, "encoder", applicationContext, rootEventId)
            { _: CodecParameters, _: ApplicationContext, _: EventID? ->
                SyncEncoder(configuration.encoder!!, applicationContext,
                    EncodeProcessor(
                        applicationContext.codecFactory,
                        applicationContext.codecSettings,
                        applicationContext.protoToIMessageConverter
                    ),
                    rootEventId).also { it.start(configuration.rabbitMQ) }
            }
        }
        createGeneralDecoder(applicationContext, configuration, rootEventId)
        createGeneralEncoder(applicationContext, configuration, rootEventId)
        logger.info { "codec started" }
    }

    private fun createGeneralEncoder(context: ApplicationContext, configuration: Configuration, rootEventId: EventID?) {
        val generalEncodeParameters = createGeneralEncodeParameters(configuration)
        createAndStartCodec (generalEncodeParameters, "general-encoder", context, rootEventId
        )
        { _: CodecParameters, _: ApplicationContext, _: EventID? ->
            SyncEncoder(
                generalEncodeParameters, context,
                EncodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.protoToIMessageConverter
                ),
                rootEventId
            ).also { it.start(configuration.rabbitMQ) }
        }
    }

    private fun createGeneralDecoder(context: ApplicationContext, configuration: Configuration, rootEventId: EventID?) {
        val generalDecodeParameters = createGeneralDecodeParameters(configuration)
        createAndStartCodec (generalDecodeParameters, "general-decoder", context, rootEventId
        )
        { _: CodecParameters, _: ApplicationContext, _: EventID? ->
            SyncDecoder(
                generalDecodeParameters, context,
                DecodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.messageToProtoConverter
                ),
                rootEventId
            ).also { it.start(configuration.rabbitMQ) }
        }
    }

    private fun createAndStartCodec(
        codecParameters: CodecParameters,
        codecName: String,
        applicationContext: ApplicationContext,
        rootEventId: EventID?,
        creationFunction: (CodecParameters, ApplicationContext, EventID?) -> AutoCloseable
    ) {
        val codecInstance = creationFunction.invoke(codecParameters, applicationContext, rootEventId)
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

    private fun createGeneralDecodeParameters(configuration: Configuration): CodecParameters {
        return CodecParameters(
            InputParameters(configuration.generalExchangeName, configuration.generalDecodeInQueue),
            OutputParameters(
                listOf(
                    Filter(
                        configuration.generalExchangeName,
                        configuration.generalDecodeOutQueue,
                        null
                    )
                )
            ))
    }

    private fun createGeneralEncodeParameters(configuration: Configuration): CodecParameters {
        return CodecParameters(
            InputParameters(configuration.generalExchangeName, configuration.generalEncodeInQueue),
            OutputParameters(
                listOf(
                    Filter(
                        configuration.generalExchangeName,
                        configuration.generalEncodeOutQueue,
                        null
                    )
                )
            ))
    }

    private fun createAndStoreRootEvent(applicationContext: ApplicationContext): EventID? {
        val eventConnector = applicationContext.eventConnector
        if (eventConnector != null) {
            try {
                val storeEventFuture = eventConnector.storeEvent(
                    StoreEventRequest.newBuilder()
                        .setEvent(
                            Event.newBuilder()
                                .setStatus(SUCCESS)
                                .setName("Codec_${applicationContext.codec::class.java.simpleName}" +
                                        "_${LocalDateTime.now()}")
                                .setType("CodecRoot")
                                .build()
                        )
                        .build()
                )
                return EventID.newBuilder().setId(storeEventFuture.get().id.value).build()
            } catch (exception: Exception) {
                logger.warn(exception) { "could not store root event" }
            }
        }
        return null
    }
}

