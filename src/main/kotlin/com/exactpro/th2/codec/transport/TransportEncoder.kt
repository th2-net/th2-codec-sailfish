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
package com.exactpro.th2.codec.transport

import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.util.extractMessageIds
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import mu.KotlinLogging

class TransportEncoder(
    router: MessageRouter<GroupBatch>,
    applicationContext: ApplicationContext,
    sourceAttributes: String,
    targetAttributes: String,
    private val processor: AbstractCodecProcessor<GroupBatch, ParsedMessage, RawMessage>,
) : AbstractTransportCodec(
    router,
    applicationContext,
    sourceAttributes,
    targetAttributes
) {
    private val protocol = processor.protocol

    override fun getDirection(): Direction = Direction.ENCODE
    override fun checkResultBatch(resultBatch: GroupBatch): Boolean = resultBatch.groups.isNotEmpty()

    override fun checkResult(protoResult: MessageGroup): Boolean = protoResult.messages.isNotEmpty()

    override fun isTransformationComplete(result: GroupBatch): Boolean = result.groups.asSequence()
        .flatMap(MessageGroup::messages)
        .all { it is RawMessage }

    override fun processMessageGroup(batch: GroupBatch, group: MessageGroup): MessageGroup? {
        if (group.messages.isEmpty()) {
            return null
        }

        if (!group.isEncodable()) {
            LOGGER.debug {
                "No messages of $protocol protocol or mixed empty and non-empty protocols are present, ids ${group.extractMessageIds}"
            }
            return group
        }

        return MessageGroup.builder().apply {
            group.messages.forEach { message ->
                if (message is ParsedMessage && checkProtocol(message, protocol)) {
                    addMessage(processor.process(batch, message))
                } else {
                    addMessage(message)
                }
            }
        }.build()
    }

    private fun MessageGroup.isEncodable() = messages.asSequence()
        .filter { it is ParsedMessage && checkProtocol(it, protocol) }
        .firstOrNull() != null

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

