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
import com.exactpro.th2.codec.util.codecContext
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.codec.util.toHexString
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessage
import com.exactpro.th2.infra.grpc.RawMessageBatch
import mu.KotlinLogging

/**
 * This processor joins data from all messages in [RawMessageBatch] to a single buffer.
 * After that, it decodes the cumulated buffer using [com.exactpro.sf.externalapi.codec.IExternalCodec].
 * The result of the decoding must produce the same number of messages as [RawMessageBatch] has.
 * Also, the raw data from decoded messages must match the data from corresponded [RawMessage].
 */
class CumulativeDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    messageToProtoConverter: IMessageToProtoConverter
) : RawBatchDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter) {

    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessageBatch): MessageBatch {
        try {
            val batchData = joinBatchData(source)
            val messageBatchBuilder = MessageBatch.newBuilder()
            val decodedMessageList = getCodec().decode(batchData, source.codecContext)
            logger.debug {
                "decoded messages: {${decodedMessageList.joinToString { message -> "${message.name}: $message" }}}"
            }
            if (checkSizeAndContext(source, decodedMessageList)) {
                for (pair in source.messagesList.zip(decodedMessageList)) {
                    val metadataBuilder = toMessageMetadataBuilder(pair.first)
                    val protoMessageBuilder = messageToProtoConverter.toProtoMessage(pair.second)
                    protoMessageBuilder.metadata = metadataBuilder.setMessageType(pair.second.name).build()
                    messageBatchBuilder.addMessages(protoMessageBuilder)
                }
            }
            return messageBatchBuilder.build()
        } catch (exception: Exception) {
            // FIXME wrap and rethrow exception
            logger.error(exception) { "could not process ${source.toDebugString()}" }
            return MessageBatch.newBuilder().build()
        }
    }

    private fun checkSizeAndContext(source: RawMessageBatch, decodedMessageList: List<IMessage>): Boolean {
        if (source.messagesCount != decodedMessageList.size) {
            logger.error {
                "messages size mismatch: source count = ${source.messagesCount}," +
                        " decoded count = ${decodedMessageList.size}"
            }
            return false
        }
        for ((index, pair) in source.messagesList.zip(decodedMessageList).withIndex()) {
            val sourceMessageData = pair.first.body.toByteArray()
            val decodedMessageData = pair.second.metaData.rawMessage
            if (decodedMessageData != null && !(sourceMessageData contentEquals decodedMessageData)) {
                logger.error {
                    "content mismatch by position $index in the batch: " +
                            "source hex: '${sourceMessageData.toHexString()}', " +
                            "decoded hex: '${decodedMessageData.toHexString()}'"
                }
                return false
            }
        }
        return true
    }

    private fun joinBatchData(rawMessageBatch: RawMessageBatch): ByteArray {
        var batchData = ByteArray(0)
        for (rawMessage in rawMessageBatch.messagesList) {
            batchData += rawMessage.body.toByteArray()
        }
        return batchData
    }
}