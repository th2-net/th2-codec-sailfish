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

package com.exactpro.th2.codec.sailfish.transport

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.util.EvolutionBatch
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.sf.messages.service.ErrorMessage
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.sailfish.AbstractCodecProcessor
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.codec.sailfish.util.toCodeContext
import com.exactpro.th2.codec.util.toErrorMessage
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import mu.KotlinLogging

class TransportDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val messageConverter: IMessageToTransportConverter
) : AbstractCodecProcessor<RawMessage, List<ParsedMessage.FromMapBuilder>>(codecFactory, codecSettings) {

    override fun process(message: RawMessage, context: IReportingContext): List<ParsedMessage.FromMapBuilder> {
        try {
            val data: ByteArray = message.body.toByteArray()
            LOGGER.debug { "Start decoding message with id: '${message.id}'" }
            LOGGER.trace { "Decoding message: $message" }

            val decodedMessages: List<IMessage> = codec.decode(data, message.toCodeContext())
                .flatMap {
                    if (it.name == EvolutionBatch.MESSAGE_NAME) {
                        EvolutionBatch(it).batch
                    } else {
                        listOf(it)
                    }
                }

            checkErrorMessageContains(decodedMessages, context)
            checkRawData(decodedMessages, data)
            LOGGER.debug { "Message with id: '${message.id}' successfully decoded" }
            LOGGER.trace { "Decoded messages: $decodedMessages" }

            return decodedMessages.map { msg ->
                messageConverter.toTransportBuilder(msg).apply {
                    setId(message.id)
                    message.eventId?.let { setEventId(it) }
                    setProtocol(this@TransportDecodeProcessor.protocol)
                    setMetadata(message.metadata)
                }
            }
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Cannot decode message from $message. Creating th2-codec-error message with description." }
            context.warning(
                "Cannot decode message: ${ex.message ?: "blank error message"}. $ERROR_TYPE_MESSAGE with cause published instead"
            )
            // FIXME: create builder instead of convert to builder
            return listOf(message.toErrorMessage(protocols, message.eventId?.toProto(), ex.message ?: ex.toString()).toBuilder())
        }
    }

    private fun checkErrorMessageContains(
        decodedMessages: List<IMessage>,
        context: IReportingContext
    ) {
        for (msg in decodedMessages) {
            if (msg.name == ErrorMessage.MESSAGE_NAME) {
                context.warning("Error during decode msg: ${msg.getField<String>("Cause")}")
            }
        }
    }

    private fun checkRawData(decodedMessages: List<IMessage>, originalData: ByteArray) {
        if (decodedMessages.isEmpty()) {
            throw DecodeException("No message was decoded")
        }
        val totalDecodedRawSize = decodedMessages.sumOf {
            val size = requireNotNull(it.metaData.rawMessage) { "Raw data is null for message: ${it.name}" }.size
            check(size > 0) { "Message ${it.name} has empty raw data" }
            size
        }
        if (originalData.size != totalDecodedRawSize) {
            throw DecodeException(
                "The decoded raw data total size is different from the original one. " +
                        "Decoded ($totalDecodedRawSize): ${decodedMessages.map { it.metaData.rawMessage?.contentToString() }}, " +
                        "Original (${originalData.size}): ${originalData.contentToString()}"
            )
        }
        val rawMessage = collectToByteArray(originalData.size, decodedMessages)
        if (!(rawMessage contentEquals originalData)) {
            throw DecodeException(
                "The decoded raw data is different from the original one. " +
                        "Decoded: ${rawMessage.contentToString()}, Original: ${originalData.contentToString()}"
            )
        }
    }

    private fun collectToByteArray(totalSize: Int, decodedMessages: List<IMessage>): ByteArray {
        return ByteArray(totalSize).also { dest ->
            var destIndex = 0
            decodedMessages.forEach {
                // should be never happened here because we checked it earlier
                val bytes = requireNotNull(it.metaData.rawMessage) { "Raw data for message ${it.name} is null" }
                bytes.copyInto(dest, destIndex)
                destIndex += bytes.size
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}