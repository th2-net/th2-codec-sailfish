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
        configuration.transportLines.forEach { (prefix, type) ->
            when (type) {
                PROTOBUF -> {
                    add(createProtoDecoder(prefix))
                    add(createProtoEncoder(prefix))
                }

                TH2_TRANSPORT -> {
                    add(createTransportDecoder(prefix))
                    add(createTransportEncoder(prefix))
                }
            }
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
        prefix: String,
    ): AutoCloseable = TransportEncoder(
        transportRouter,
        context,
        "${prefix}_encoder_in",
        "${prefix}_encoder_out",
        TransportEncodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.transportToIMessageConverter
        )
    ).also { K_LOGGER.info { "Transport '${prefix}_encoder' started" } }

    private fun createTransportDecoder(
        prefix: String,
    ): AutoCloseable = TransportDecoder(
        transportRouter,
        context,
        "${prefix}_decoder_in",
        "${prefix}_decoder_out",
        TransportDecodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.messageToTransportConverter,
            context.eventBatchCollector
        )
    ).also { K_LOGGER.info { "Transport '${prefix}_decoder' started" } }

    private fun createProtoEncoder(
        prefix: String,
    ): AutoCloseable = ProtoEncoder(
        protoRouter,
        context,
        "${prefix}_encoder_in",
        "${prefix}_encoder_out",
        ProtoEncodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.protoToIMessageConverter
        )
    ).also { K_LOGGER.info { "Proto '${prefix}_encoder' started" } }

    private fun createProtoDecoder(
        prefix: String,
    ): AutoCloseable = ProtoDecoder(
        protoRouter,
        context,
        "${prefix}_decoder_in",
        "${prefix}_decoder_out",
        ProtoDecodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.messageToProtoConverter,
            context.eventBatchCollector
        )
    ).also { K_LOGGER.info { "Proto '${prefix}_decoder' started" } }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}