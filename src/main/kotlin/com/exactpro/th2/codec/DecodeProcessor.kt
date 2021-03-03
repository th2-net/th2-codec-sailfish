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
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

class DecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val messageToProtoConverter: IMessageToProtoConverter
) :  AbstractCodecProcessor<RawMessage, Message.Builder?>(codecFactory, codecSettings) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessage): Message.Builder? {
        try {
            val data: ByteArray = source.body.toByteArray()
            logger.debug { "Decoding message: ${source.toDebugString()}" }
            val decodedMessages = getCodec().decode(data, source.toCodecContext())
            logger.debug { "Decoded messages: $decodedMessages" }
            val decodedMessage: IMessage = checkCountAndRawData(decodedMessages, data)

            return messageToProtoConverter.toProtoMessage(decodedMessage).apply {
                if (source.hasParentEventId()) {
                    parentEventId = source.parentEventId
                }
                metadata = toMessageMetadataBuilder(source).apply {
                    messageType = decodedMessage.name
                }.build()
            }
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $source" }
            return null
        }
    }

    private fun toMessageMetadataBuilder(sourceMessage: RawMessage): MessageMetadata.Builder {
        return MessageMetadata.newBuilder()
            .setId(sourceMessage.metadata.id)
            .setTimestamp(sourceMessage.metadata.timestamp)
            .putAllProperties(sourceMessage.metadata.propertiesMap)
    }

    /**
     * Checks that the [decodedMessages] contains exact one message and its raw data is the same as [originalData].
     */
    private fun checkCountAndRawData(decodedMessages: List<IMessage>, originalData: ByteArray): IMessage {
        val decodedMessage = when {
            decodedMessages.size == 1 -> decodedMessages[0]
            decodedMessages.isEmpty() -> throw DecodeException("No message was decoded")
            else -> throw DecodeException("More than one message was decoded: ${decodedMessages.size}")
        }
        val rawMessage = decodedMessage.metaData.rawMessage
            ?: throw DecodeException("Raw data is null for message: ${decodedMessage.name}")
        return if (rawMessage contentEquals originalData) {
            decodedMessage
        } else {
            throw DecodeException("The decoded raw data is different from the original one. " +
                    "Decoded: ${rawMessage.contentToString()}, Original: ${originalData.contentToString()}")
        }
    }

}