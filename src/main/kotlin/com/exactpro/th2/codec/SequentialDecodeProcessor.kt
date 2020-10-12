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

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.IMessageToProtoConverter
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessage
import com.exactpro.th2.infra.grpc.RawMessageBatch
import mu.KotlinLogging

/**
 * This processor decoded each [RawMessage] from [RawMessageBatch] separately.
 * The raw data of the decoded message must
 */
class SequentialDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    messageToProtoConverter: IMessageToProtoConverter
) : RawBatchDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessageBatch): MessageBatch {
        val messageBatch: MessageBatch.Builder = MessageBatch.newBuilder()
        for (rawMessage in source.messagesList) {
            processSingle(rawMessage)?.also {
                messageBatch.addMessages(it)
            }
        }
        return messageBatch.build().also {
            val sourceCount = source.messagesCount
            val resultCount = it.messagesCount
            if (sourceCount != resultCount) {
                logger.warn { "The size of the input batch and output batch are not equal (Source: $sourceCount, Result: $resultCount)" }
            }
        }
    }

    private fun processSingle(rawMessage: RawMessage): Message? {
        try {
            val data = rawMessage.toByteArray()
            val decodedMessages = getCodec().decode(data, rawMessage.toCodecContext())
            logger.debug { "Decoded messages: $decodedMessages" }
            val decodedMessage: IMessage = checkCountAndRawData(decodedMessages, data)

            val messageMetadata = toMessageMetadataBuilder(rawMessage)
                .setMessageType(decodedMessage.name)
                .build()
            return messageToProtoConverter.toProtoMessage(decodedMessage)
                .setMetadata(messageMetadata)
                .build()
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $rawMessage" }
            return null
        }
    }

    /**
     * Checks that the [decodedMessages] contains exact one message and its raw data is the same as [originalData].
     */
    private fun checkCountAndRawData(decodedMessages: List<IMessage>, originalData: ByteArray): IMessage {
        val decodedMessage = when {
            decodedMessages.size == 1 -> decodedMessages[0]
            decodedMessages.isEmpty() -> throw DecodeException("No message was decoded")
            else -> throw DecodeException("More than one message is decoded: ${decodedMessages.size}")
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