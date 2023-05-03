/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.proto

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.IMessageFactory
import com.exactpro.sf.common.util.EvolutionBatch
import com.exactpro.sf.extensions.get
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.sf.messages.service.ErrorMessage
import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.EventBatchCollector
import com.exactpro.th2.codec.Th2IMessage
import com.exactpro.th2.codec.Th2MessageFactory
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.codec.util.toErrorMessage
import com.exactpro.th2.codec.util.toMessageMetadataBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

class ProtoDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    createMessageFactory: Th2MessageFactory<*>,
    private val eventBatchCollector: EventBatchCollector
) : AbstractCodecProcessor<MessageGroupBatch, RawMessage, List<Message.Builder>>(codecFactory, codecSettings, createMessageFactory) {
    private val logger = KotlinLogging.logger { }
    override val protocol = codecFactory.protocolName

    override fun process(batch: MessageGroupBatch, message: RawMessage): List<Message.Builder> {
        try {
            val data: ByteArray = message.body.toByteArray()
            logger.debug { "Start decoding message with id: '${message.metadata.id.toJson()}'" }
            logger.trace { "Decoding message: ${message.toJson()}" }

            val decodedMessages = getCodec().decode(data, message.toCodecContext())
                .flatMap {
                    if (it.name == EvolutionBatch.MESSAGE_NAME) {
                        EvolutionBatch(it).batch
                    } else {
                        listOf(it)
                    }
                }

            checkErrorMessageContains(decodedMessages, message)
            checkRawData(decodedMessages, data)
            logger.trace { "Decoded messages: $decodedMessages" }
            logger.debug { "Message with id: '${message.metadata.id.toJson()}' successfully decoded" }

            return decodedMessages.map { msg ->
                check(msg is Th2IMessage<*> && msg.getMessage() is Message.Builder) {
                    "Error while decoding Proto message. Wrong IMessage wrapper used: ${msg::class.java}"
                }
                (msg.getMessage() as Message.Builder).apply {
                    if (message.hasParentEventId()) {
                        parentEventId = message.parentEventId
                    }
                    metadata = message.toMessageMetadataBuilder(protocol).apply {
                        messageType = msg.name
                    }.build()
                }
            }
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from ${message.toJson()}. Creating th2-codec-error message with description." }
            eventBatchCollector.createAndStoreDecodeErrorEvent(
                "Cannot decode message: ${ex.message ?: "blank error message"}. $ERROR_TYPE_MESSAGE with cause published instead",
                message, ex
            )
            return listOf(message.toErrorMessage(ex, protocol))
        }
    }

    private fun checkErrorMessageContains(decodedMessages: List<IMessage>, rawMessage: RawMessage) {
        for (msg in decodedMessages) {
            if (msg.name == ErrorMessage.MESSAGE_NAME) {
                eventBatchCollector.createAndStoreDecodeErrorEvent(
                    "Error during decode msg: ${msg.getField<String>("Cause")}", rawMessage
                )
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
                // should never happened here because we checked it earlier
                val bytes = requireNotNull(it.metaData.rawMessage) { "Raw data for message ${it.name} is null" }
                bytes.copyInto(dest, destIndex)
                destIndex += bytes.size
            }
        }
    }

}