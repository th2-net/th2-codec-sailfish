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

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.IMessageFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.AbstractCodecProcessor
import com.exactpro.th2.codec.Th2MessageFactory
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.sailfish.utils.MessageWrapper
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.ByteString
import mu.KotlinLogging

class ProtoEncodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    private val customFactory: Th2MessageFactory<Message.Builder>
) : AbstractCodecProcessor<MessageGroupBatch, Message, RawMessage.Builder>(codecFactory, codecSettings, customFactory) {
    private val logger = KotlinLogging.logger { }
    override val protocol = codecFactory.protocolName

    override fun process(batch: MessageGroupBatch, message: Message): RawMessage.Builder {
        val convertedSourceMessage = customFactory.createMessage(message.toBuilder())

        val encodedMessageData = getCodec().encode(convertedSourceMessage, message.toCodecContext())
        return RawMessage.newBuilder().apply {
            if (message.hasParentEventId()) {
                parentEventId = message.parentEventId
            }
            body = ByteString.copyFrom(encodedMessageData)
            metadata = toRawMessageMetadataBuilder(message).also {
                logger.debug { "message metadata: ${it.toJson()}" }
            }
        }
    }

    private fun toRawMessageMetadataBuilder(sourceMessage: Message): RawMessageMetadata {
        return RawMessageMetadata.newBuilder()
            .setId(sourceMessage.metadata.id)
            .setProtocol(protocol)
            .putAllProperties(sourceMessage.metadata.propertiesMap)
            .build()
    }
}