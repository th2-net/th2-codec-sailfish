/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.configuration.TransportType.PROTOBUF
import com.exactpro.th2.codec.configuration.TransportType.TH2_TRANSPORT
import com.exactpro.th2.codec.proto.ProtoDecodeProcessor
import com.exactpro.th2.codec.proto.ProtoDecoder
import com.exactpro.th2.codec.proto.ProtoEncodeProcessor
import com.exactpro.th2.codec.proto.ProtoEncoder
import com.exactpro.th2.codec.transport.TransportDecodeProcessor
import com.exactpro.th2.codec.transport.TransportDecoder
import com.exactpro.th2.codec.transport.TransportEncodeProcessor
import com.exactpro.th2.codec.transport.TransportEncoder
import com.exactpro.th2.common.schema.factory.CommonFactory
import mu.KotlinLogging

class Application(commonFactory: CommonFactory) : AutoCloseable {

    private val configuration = Configuration.create(commonFactory)
    private val context = ApplicationContext.create(configuration, commonFactory)
    private val protoRouter = context.commonFactory.messageRouterMessageGroupBatch
    private val transportRouter = context.commonFactory.transportGroupBatchRouter

    private val codecs: List<AutoCloseable> = mutableListOf<AutoCloseable>().apply {
        fun String.withSuffix(suffix: String): String {
            return if (isBlank()) {
                suffix
            } else {
                "${this}_$suffix"
            }
        }
        configuration.transportLines.forEach { (prefix, type) ->
            add(
                when (type) {
                    PROTOBUF -> ::createProtoDecoder
                    TH2_TRANSPORT -> ::createTransportDecoder
                }(
                    prefix.withSuffix("decoder"),
                    prefix.withSuffix("decoder_in"),
                    prefix.withSuffix("decoder_out"),
                )
            )
            add(
                when (type) {
                    PROTOBUF -> ::createProtoEncoder
                    TH2_TRANSPORT -> ::createTransportEncoder
                }(
                    prefix.withSuffix("encoder"),
                    prefix.withSuffix("encoder_in"),
                    prefix.withSuffix("encoder_out"),
                )
            )
        }
    }

    override fun close() {
        codecs.forEach { codec ->
            runCatching(codec::close).onFailure {
                K_LOGGER.error(it) { "Codec closing failure" }
            }
        }

        runCatching(context::close).onFailure {
            K_LOGGER.error(it) { "Application context closing failure" }
        }
    }

    private fun createTransportEncoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): AutoCloseable = TransportEncoder(
        transportRouter,
        context,
        sourceAttributes,
        targetAttributes,
        TransportEncodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.transportToIMessageConverter
        )
    ).also { K_LOGGER.info { "Transport '$codecName' started" } }

    private fun createTransportDecoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): AutoCloseable = TransportDecoder(
        transportRouter,
        context,
        sourceAttributes,
        targetAttributes,
        TransportDecodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.messageToTransportConverter,
            context.eventBatchCollector
        )
    ).also { K_LOGGER.info { "Transport '$codecName' started" } }

    private fun createProtoEncoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): AutoCloseable = ProtoEncoder(
        protoRouter,
        context,
        sourceAttributes,
        targetAttributes,
        ProtoEncodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.protoToIMessageConverter
        )
    ).also { K_LOGGER.info { "Proto '$codecName' started" } }

    private fun createProtoDecoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): AutoCloseable = ProtoDecoder(
//        "${prefix}_decoder", "${prefix}_decoder_in", "${prefix}_decoder_out"
        protoRouter,
        context,
        sourceAttributes,
        targetAttributes,
        ProtoDecodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.messageToProtoConverter,
            context.eventBatchCollector
        )
    ).also { K_LOGGER.info { "Proto '$codecName' started" } }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}