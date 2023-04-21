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
import com.exactpro.th2.common.schema.factory.CommonFactory
import mu.KotlinLogging

class Application(commonFactory: CommonFactory) : AutoCloseable {

    private val configuration = Configuration.create(commonFactory)
    private val context = ApplicationContext.create(configuration, commonFactory)
    private val groupRouter = context.commonFactory.messageRouterMessageGroupBatch

    private val decoder = createDecoder("decoder", configuration.decoderInputAttribute, configuration.decoderOutputAttribute)
    private val encoder = createEncoder("encoder", configuration.encoderInputAttribute, configuration.encoderOutputAttribute)

    private val generalDecoder = createDecoder("general-decoder", configuration.generalDecoderInputAttribute, configuration.generalDecoderOutputAttribute)
    private val generalEncoder = createEncoder("general-encoder", configuration.generalEncoderInputAttribute, configuration.generalEncoderOutputAttribute)
    override fun close() {
        runCatching(generalEncoder::close).onFailure {
            K_LOGGER.error(it) { "General encoder closing failure" }
        }
        runCatching(generalDecoder::close).onFailure {
            K_LOGGER.error(it) { "General decoder closing failure" }
        }
        runCatching(encoder::close).onFailure {
            K_LOGGER.error(it) { "Encoder closing failure" }
        }
        runCatching(decoder::close).onFailure {
            K_LOGGER.error(it) { "Decoder closing failure" }
        }

        runCatching(context::close).onFailure {
            K_LOGGER.error(it) { "Application context closing failure" }
        }
    }

    private fun createEncoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): SyncEncoder = SyncEncoder(
        groupRouter, context,
        EncodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.protoToIMessageConverter,
            context.eventBatchCollector
        ),
        configuration.enableVerticalScaling
    ).also {
        it.start(sourceAttributes, targetAttributes)
        K_LOGGER.info { "'$codecName' started" }
    }

    private fun createDecoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
    ): SyncDecoder = SyncDecoder(
        groupRouter, context,
        DecodeProcessor(
            context.codecFactory,
            context.codecSettings,
            context.messageToProtoConverter,
            context.eventBatchCollector
        ),
        configuration.enableVerticalScaling
    ).also {
        it.start(sourceAttributes, targetAttributes)
        K_LOGGER.info { "'$codecName' started" }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}