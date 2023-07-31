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

import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.util.toCodeContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.MessageWrapper
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter
import mu.KotlinLogging

class TransportEncodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val converter: TransportToIMessageConverter
) : AbstractCodecProcessor<GroupBatch, ParsedMessage, RawMessage>(codecFactory, codecSettings) {
    private val logger = KotlinLogging.logger { }
    override val protocol = codecFactory.protocolName

    override fun process(batch: GroupBatch, message: ParsedMessage): RawMessage {
        val convertedSourceMessage: MessageWrapper =
            converter.fromTransport(batch.book, batch.sessionGroup, message, true).also {
                logger.debug { "converted source message '${it.name}': $it" }
            }

        val encodedMessageData = getCodec().encode(convertedSourceMessage, message.toCodeContext())
        return RawMessage.builder().apply {
            setId(message.id)
            message.eventId?.let { setEventId(it) }
            setMetadata(message.metadata)
            setProtocol(message.protocol)
            setBody(encodedMessageData)
        }.build()
    }
}