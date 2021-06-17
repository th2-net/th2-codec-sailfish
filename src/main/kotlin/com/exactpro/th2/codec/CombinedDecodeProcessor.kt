/*
 *  Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.sf.messages.service.ErrorMessage
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.codec.util.toHexString
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

/**
 * This processor joins data from all messages in [RawMessageBatch] to a single buffer for FIRST direction
 * and decodes messages sequentially for SECOND direction.
 *
 * NOTE: if the raw message that was decoded sequentially produces more than one parsed message
 * the one to publish will be chosen according to the following rule:
 * + if the only one parsed message is a business message it will be published
 * + if there is more than one admin message in the result (and not business messages)
 *   or more than one business message in the result the error will be reported
 */
class CombinedDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    messageToProtoConverter: IMessageToProtoConverter
) : RawBatchDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter) {

    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessageBatch): MessageBatch {
        try {
            val directionGroups = groupsByDirectionInOrder(source)
            val messageBatchBuilder = MessageBatch.newBuilder()
            for ((direction, sourceMessages) in directionGroups) {
                val decodeMapping: List<Pair<RawMessage, IMessage>> = when (direction) {
                    Direction.FIRST -> decodedCumulative(sourceMessages)
                    Direction.SECOND -> decodedSequentialWithFiltering(sourceMessages)
                    Direction.UNRECOGNIZED -> throw IllegalStateException("Unexpected direction: $direction")
                }
                for ((raw, parsed) in decodeMapping) {
                    val metadataBuilder = toMessageMetadataBuilder(raw)
                    val protoMessageBuilder = messageToProtoConverter.toProtoMessage(parsed)
                    if (raw.hasParentEventId()) {
                        protoMessageBuilder.parentEventId = raw.parentEventId
                    }
                    protoMessageBuilder.metadata = metadataBuilder.setMessageType(parsed.name).build()
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

    private fun decodedSequentialWithFiltering(sourceMessages: List<RawMessage>): List<Pair<RawMessage, IMessage>> =
        sourceMessages.map {
            val decoded = getCodec().decode(it.rawBody, it.toCodecContext())
            logger.debug { "decoded messages: {${decoded.toHumanReadable()}}" }
            if (checkContent(it, decoded)) {
                val message = getMatchedMessage(decoded)
                return@map it to (message ?: ErrorMessage()
                    .setCause("Cannot determinate the message to choose from decoded results: ${decoded.toHumanReadable()}")
                    .message)
            }
            return@map it to ErrorMessage()
                .setCause("The content for decoded messages does not matches the original one")
                .message
        }

    private fun decodedCumulative(sourceMessages: List<RawMessage>): List<Pair<RawMessage, IMessage>> {
        val batchData = joinBatchData(sourceMessages)
        val decodedMessageList: List<IMessage> = getCodec().decode(batchData, sourceMessages.first().toCodecContext())
        logger.debug { "decoded messages: {${decodedMessageList.toHumanReadable()}}" }
        return if (checkSizeAndContent(sourceMessages, decodedMessageList)) {
            sourceMessages.zip(decodedMessageList)
        } else {
            listOf()
        }
    }

    private fun groupsByDirectionInOrder(source: RawMessageBatch): List<DirectionGroup> {
        val groups = arrayListOf<DirectionGroup>()
        val currentGroup = arrayListOf<RawMessage>()
        var prevDirection: Direction? = null
        source.messagesList.forEach {
            val direction: Direction = it.metadata.id.direction
            val prev = prevDirection ?: direction
            if (direction != prev) {
                groups += DirectionGroup(prev, currentGroup.toList())
                currentGroup.clear()
            }
            currentGroup += it
            prevDirection = direction
        }
        if (currentGroup.isNotEmpty()) {
            groups += DirectionGroup(requireNotNull(prevDirection) { "Group is not empty but direction is null" }, currentGroup)
        }
        return groups
    }

    private fun checkSizeAndContent(source: List<RawMessage>, decodedMessageList: List<IMessage>): Boolean {
        if (source.size != decodedMessageList.size) {
            logger.error {
                "messages size mismatch: source count = ${source.size}," +
                        " decoded count = ${decodedMessageList.size}"
            }
            return false
        }
        for ((index, pair) in source.zip(decodedMessageList).withIndex()) {
            val sourceMessageData = pair.first.rawBody
            val decodedMessageData = pair.second.metaData.rawMessage
            if (decodedMessageData != null && !(sourceMessageData contentEquals decodedMessageData)) {
                logger.error {
                    "content mismatch by position $index in the batch for message ${pair.first.metadata.id.toDebugString()}: " +
                            "source hex: '${sourceMessageData.toHexString()}', " +
                            "decoded hex: '${decodedMessageData.toHexString()}'"
                }
                return false
            }
        }
        return true
    }

    private fun checkContent(source: RawMessage, decodedMessageList: List<IMessage>): Boolean {
        val joinedData = joinBatchData(decodedMessageList) { it.metaData.rawMessage!! }
        val sourceMessageData = source.rawBody
        if (!(sourceMessageData contentEquals joinedData)) {
            logger.error {
                "content mismatch by position in the batch for message ${source.metadata.id.toDebugString()}: " +
                    "source hex: '${sourceMessageData.toHexString()}', " +
                    "decoded hex: '${joinedData.toHexString()}'"
            }
            return false
        }
        return true
    }

    private fun getMatchedMessage(messages: List<IMessage>): IMessage? {
        if (messages.size == 1) {
            return messages.first()
        }
        val businessMsgs = messages.filter { !it.metaData.isAdmin }
        if (businessMsgs.size == 1) {
            return businessMsgs.first()
        }
        logger.error { "There is more than one business message (${businessMsgs.toHumanReadable()}) in the decoded messages: ${messages.toHumanReadable()}" }
        return null
    }

    private fun List<IMessage>.toHumanReadable(): String = joinToString { "${it.name}: $it" }

    private fun joinBatchData(rawMessages: List<RawMessage>): ByteArray {
        return joinBatchData(rawMessages) { it.rawBody }
    }

    private fun <T> joinBatchData(rawMessages: List<T>, getBytes: (T) -> ByteArray): ByteArray {
        var batchData = ByteArray(0)
        for (rawMessage in rawMessages) {
            batchData += getBytes(rawMessage)
        }
        return batchData
    }

    private val RawMessage.rawBody: ByteArray
        get() = body.toByteArray()

    private class DirectionGroup(val direction: Direction, val messages: List<RawMessage>) {
        operator fun component1(): Direction = direction
        operator fun component2(): List<RawMessage> = messages
    }
}