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

import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging

class ProtoEncoder(
    router: MessageRouter<MessageGroupBatch>,
    applicationContext: ApplicationContext,
    sourceAttributes: String,
    targetAttributes: String,
    private val processor: AbstractCodecProcessor<MessageGroupBatch, Message, RawMessage.Builder>,
) : AbstractProtoCodec(
    router,
    applicationContext,
    sourceAttributes,
    targetAttributes
) {
    private val protocol = processor.protocol

    override fun getDirection(): Direction = Direction.ENCODE

    override fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean = resultBatch.groupsCount > 0

    override fun checkResult(protoResult: MessageGroup): Boolean = protoResult.messagesCount > 0

    override fun isTransformationComplete(result: MessageGroupBatch): Boolean = result.groupsList.asSequence()
        .flatMap(MessageGroup::getMessagesList)
        .all(AnyMessage::hasRawMessage)

    override fun processMessageGroup(batch: MessageGroupBatch, messageGroup: MessageGroup): MessageGroup? {
        if (messageGroup.messagesCount < 1) {
            return null
        }

        if (!messageGroup.isEncodable()) {
            LOGGER.debug { "No messages of $protocol protocol or mixed empty and non-empty protocols are present, ids ${messageGroup.messageIds}" }
            return messageGroup
        }

        val groupBuilder = MessageGroup.newBuilder()

        for (notTypeMessage in messageGroup.messagesList) {
            if (notTypeMessage.hasMessage()) {
                val message = notTypeMessage.message
                if (checkProtocol(message)) {
                    groupBuilder += processor.process(batch, message)
                    continue
                }
            }
            groupBuilder.addMessages(notTypeMessage)
        }

        return groupBuilder.build()
    }

    private fun checkProtocol(message: Message) = message.metadata.protocol.let {
        it.isNullOrEmpty() || processor.protocol.equals(it, ignoreCase = true)
    }

    private fun MessageGroup.isEncodable(): Boolean {
        val protocols = messagesList.asSequence()
            .filter(AnyMessage::hasMessage)
            .map { it.message.metadata.protocol }
            .toList()

        return protocols.all(String::isBlank) || protocols.none(String::isBlank) && protocols.any { protocol.equals(it, ignoreCase = true) }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

