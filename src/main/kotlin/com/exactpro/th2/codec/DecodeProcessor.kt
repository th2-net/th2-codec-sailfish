/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
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

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.util.EvolutionBatch
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.sf.messages.service.ErrorMessage
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_ORIGINAL_MESSAGE_TYPE
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.codec.util.toErrorMessage
import com.exactpro.th2.codec.util.toMessageMetadataBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

class DecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val messageToProtoConverter: IMessageToProtoConverter,
    private val eventBatchCollector: EventBatchCollector
) : AbstractCodecProcessor<RawMessage, List<Message.Builder>>(codecFactory, codecSettings) {
    private val logger = KotlinLogging.logger { }
    override val protocol = codecFactory.protocolName

    override fun process(source: RawMessage): List<Message.Builder> {
        try {
            val data: ByteArray = source.body.toByteArray()
            logger.debug { "Start decoding message with id: '${source.metadata.id.toJson()}'" }
            logger.trace { "Decoding message: ${source.toJson()}" }

            val decodedMessages = getCodec().decode(data, source.toCodecContext())
                .flatMap {
                    if (it.name == EvolutionBatch.MESSAGE_NAME) {
                        EvolutionBatch(it).batch
                    } else {
                        listOf(it)
                    }
                }

            checkErrorMessageContains(decodedMessages, source)
            checkRawData(decodedMessages, data)
            logger.trace { "Decoded messages: $decodedMessages" }
            logger.debug { "Message with id: '${source.metadata.id.toJson()}' successfully decoded" }

            return decodedMessages.map { msg ->
                messageToProtoConverter.toProtoMessage(msg).apply {
                    if (source.hasParentEventId()) {
                        parentEventId = source.parentEventId
                    }
                    val metadataBuilder = source.toMessageMetadataBuilder(protocol)
                    if(msg.metaData.isRejected) {
                        metadata = metadataBuilder.apply { messageType = ERROR_TYPE_MESSAGE }.build()
                        addField(ERROR_ORIGINAL_MESSAGE_TYPE, msg.name.toValue())
                        msg.metaData.rejectReason?.let {
                            addField(ERROR_CONTENT_FIELD, it.toValue())
                        }
                    } else {
                        metadata = metadataBuilder.apply { messageType = msg.name }.build()
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from ${source.toJson()}. Creating th2-codec-error message with description." }
            eventBatchCollector.createAndStoreDecodeErrorEvent(
                "Cannot decode message: ${ex.message ?: "blank error message"}. $ERROR_TYPE_MESSAGE with cause published instead",
                source, ex)
            return listOf(source.toErrorMessage(ex, protocol))
        }
    }

    private fun checkErrorMessageContains(decodedMessages: List<IMessage>, rawMessage: RawMessage) {
        for (msg in decodedMessages) {
            if (msg.name == ErrorMessage.MESSAGE_NAME) {
                eventBatchCollector.createAndStoreDecodeErrorEvent(
                    "Error during decode msg: ${msg.getField<String>("Cause")}", rawMessage
                )
            }
            if (msg.metaData.isRejected) {
                eventBatchCollector.createAndStoreDecodeErrorEvent(
                    "Reject during decode reason: ${msg.metaData.rejectReason}", rawMessage
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