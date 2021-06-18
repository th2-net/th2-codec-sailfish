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
import com.exactpro.th2.codec.util.toErrorMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

class DecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val messageToProtoConverter: IMessageToProtoConverter
) :  AbstractCodecProcessor<RawMessage, List<Message.Builder>>(codecFactory, codecSettings) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessage): List<Message.Builder> {
        try {
            val data: ByteArray = source.body.toByteArray()
            logger.debug { "Start decoding message with id: '${source.metadata.id.toDebugString()}'" }
            logger.trace { "Decoding message: ${source.toDebugString()}" }
            val decodedMessages = getCodec().decode(data, source.toCodecContext())
            checkRawData(decodedMessages, data)
            logger.trace { "Decoded messages: $decodedMessages" }
            logger.debug { "Message with id: '${source.metadata.id.toDebugString()}' successfully decoded" }

            return decodedMessages.map { msg ->
                messageToProtoConverter.toProtoMessage(msg).apply {
                    if (source.hasParentEventId()) {
                        parentEventId = source.parentEventId
                    }
                    metadata = toMessageMetadataBuilder(source).apply {
                        messageType = msg.name
                    }.build()
                }
            }
        } catch (ex: DecodeException) {
            logger.error(ex) { "Cannot decode message from $source" }
            return listOf(
                source.toErrorMessage(
                    DecodeException(
                        "Cannot decode message from ${source.metadata.id.toDebugString()}",
                        ex
                    )
                )
            )
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $source" }
            throw ex
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
    private fun checkRawData(decodedMessages: List<IMessage>, originalData: ByteArray) {
        if (decodedMessages.isEmpty()) {
            throw DecodeException("No message was decoded")
        }
        val totalDecodedRawSize = decodedMessages.sumBy {
            val size = requireNotNull(it.metaData.rawMessage) { "Raw data is null for message: ${it.name}" }.size
            check(size > 0) { "Message ${it.name} has empty raw data" }
            size
        }
        if (originalData.size != totalDecodedRawSize) {
            throw DecodeException("The decoded raw data total size is different from the original one. " +
                "Decoded ($totalDecodedRawSize): ${decodedMessages.map { it.metaData.rawMessage?.contentToString() }}, " +
                "Original (${originalData.size}): ${originalData.contentToString()}")
        }
        val rawMessage = collectToByteArray(originalData.size, decodedMessages)
        if (!(rawMessage contentEquals originalData)) {
            throw DecodeException("The decoded raw data is different from the original one. " +
                    "Decoded: ${rawMessage.contentToString()}, Original: ${originalData.contentToString()}")
        }
    }

    private fun collectToByteArray(totalSize: Int, decodedMessages: List<IMessage>): ByteArray {
        return ByteArray(totalSize).also { dest ->
            var destIndex = 0
            decodedMessages.forEach {
                // should never happen here because we checks it earlier
                val bytes = requireNotNull(it.metaData.rawMessage) { "Raw data for message ${it.name} is null" }
                bytes.copyInto(dest, destIndex)
                destIndex += bytes.size
            }
        }
    }

}