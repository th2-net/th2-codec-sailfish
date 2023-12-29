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
package com.exactpro.th2.codec.sailfish.transport

import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.sailfish.AbstractCodecProcessor
import com.exactpro.th2.codec.sailfish.AbstractRecoder
import com.exactpro.th2.codec.sailfish.util.checkProtocol
import com.exactpro.th2.codec.sailfish.util.extractMessageIds
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import mu.KotlinLogging

class TransportEncoder(
    processor: AbstractCodecProcessor<ParsedMessage, RawMessage>,
): AbstractRecoder<MessageGroup, ParsedMessage, RawMessage>(
    processor
) {
    override fun process(group: MessageGroup, context: IReportingContext): MessageGroup {
        if (!group.isEncodable()) {
            LOGGER.debug {
                "No messages of $protocol protocol or mixed empty and non-empty protocols are present, ids ${group.extractMessageIds}"
            }
            return group
        }

        return MessageGroup.builder().apply {
            group.messages.forEach { message ->
                if (message is ParsedMessage && checkProtocol(message, protocol)) {
                    addMessage(processor.process(message, context))
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

