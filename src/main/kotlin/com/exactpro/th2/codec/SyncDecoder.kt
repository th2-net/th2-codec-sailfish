/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging

class SyncDecoder(
    router: MessageRouter<MessageGroupBatch>,
    applicationContext: ApplicationContext,
    private val processor: AbstractCodecProcessor<RawMessage, List<Message.Builder>>
) : AbstractSyncCodec(
    router,
    applicationContext
) {
    private val protocol = processor.protocol

    override fun checkResult(protoResult: MessageGroup): Boolean = protoResult.messagesCount > 0
    override fun getDirection(): Direction = Direction.DECODE

    override fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean = resultBatch.groupsCount > 0

    override fun processMessageGroup(messageGroup: MessageGroup): MessageGroup? {
        if (messageGroup.messagesCount < 1) {
            return null
        }

        if (!messageGroup.isDecodable()) {
            LOGGER.debug { "No messages of $protocol protocol or mixed empty and non-empty protocols are present, ids ${messageGroup.messageIds.joinToString { shortDebugString(it) }}" }
            return messageGroup
        }

        val groupBuilder = MessageGroup.newBuilder()

        for (notTypeMessage in messageGroup.messagesList) {
            if (notTypeMessage.hasRawMessage()) {
                val rawMessage = notTypeMessage.rawMessage
                if (checkProtocol(rawMessage)) {
                    var startSeq = DEFAULT_SUBSEQUENCE_NUMBER
                    processor.process(rawMessage).forEach {
                        groupBuilder += it.apply { metadataBuilder.idBuilder.addSubsequence(startSeq++) }
                    }
                    continue
                }
            }
            groupBuilder.addMessages(notTypeMessage)
        }

        return if (groupBuilder.messagesCount > 0) groupBuilder.build() else null
    }

    private fun checkProtocol(rawMessage: RawMessage) = rawMessage.metadata.protocol.let {
        it.isNullOrEmpty() || it == processor.protocol
    }

    private fun MessageGroup.isDecodable(): Boolean {
        val protocols = messagesList.asSequence()
            .filter(AnyMessage::hasRawMessage)
            .map { it.rawMessage.metadata.protocol }
            .toList()

        return protocols.all(String::isBlank) || protocols.none(String::isBlank) && protocol in protocols
    }

    companion object {
        const val DEFAULT_SUBSEQUENCE_NUMBER = 1
        private val LOGGER = KotlinLogging.logger { }
    }
}