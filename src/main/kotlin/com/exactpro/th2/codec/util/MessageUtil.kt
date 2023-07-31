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
package com.exactpro.th2.codec.util

import com.exactpro.sf.externalapi.codec.ExternalCodecContextProperty.MESSAGE_PROPERTIES
import com.exactpro.sf.externalapi.codec.IExternalCodecContext
import com.exactpro.sf.externalapi.codec.IExternalCodecContext.Role
import com.exactpro.sf.externalapi.codec.impl.ExternalCodecContext
import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.proto
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message as TransportMessage

const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"

private fun Direction.toRole(): Role = when (this) {
    Direction.FIRST -> Role.RECEIVER
    Direction.SECOND -> Role.SENDER
    else -> throw IllegalStateException("Unsupported direction: $this")
}

private fun Role.toContext(properties: Map<String, Any> = emptyMap()): IExternalCodecContext =
    ExternalCodecContext(this, properties)

fun TransportMessage<*>.toCodeContext(): IExternalCodecContext {
    val properties = mapOf(
        MESSAGE_PROPERTIES.propertyName to metadata
    )
    return id.direction.proto.toRole().toContext(properties)
}

fun ProtoRawMessage.toCodecContext(): IExternalCodecContext {
    val properties = mapOf(
        MESSAGE_PROPERTIES.propertyName to metadata.propertiesMap
    )
    return metadata.id.direction.toRole().toContext(properties)
}

fun ProtoMessage.toCodecContext(): IExternalCodecContext {
    val properties = mapOf(
        MESSAGE_PROPERTIES.propertyName to metadata.propertiesMap
    )
    return metadata.id.direction.toRole().toContext(properties)
}

fun ProtoRawMessage.toMessageMetadataBuilder(protocol: String): MessageMetadata.Builder {
    return MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}

fun ProtoRawMessage.toErrorMessage(exception: Exception, protocol: String): ProtoMessage.Builder =
    ProtoMessage.newBuilder().apply {
        if (hasParentEventId()) {
            parentEventId = parentEventId
        }
        metadata = toMessageMetadataBuilder(protocol).setMessageType(ERROR_TYPE_MESSAGE).build()

        val content = buildString {
            var throwable: Throwable? = exception

            while (throwable != null) {
                append("Caused by: ${throwable.message}. ")
                throwable = throwable.cause
            }
        }
        putFields(ERROR_CONTENT_FIELD, Value.newBuilder().setSimpleValue(content).build())
    }

fun Message<*>.toErrorMessage(exception: Exception, protocol: String): ParsedMessage.FromMapBuilder =
    ParsedMessage.builder().apply {
        setType(ERROR_TYPE_MESSAGE)
        setId(this@toErrorMessage.id)
        this@toErrorMessage.eventId?.let { setEventId(it) }
        setMetadata(this@toErrorMessage.metadata)
        setProtocol(protocol)

        val content = buildString {
            var throwable: Throwable? = exception

            while (throwable != null) {
                append("Caused by: ${throwable.message}. ")
                throwable = throwable.cause
            }
        }
        addField(ERROR_CONTENT_FIELD, content)
    }

val MessageGroup.extractMessageIds: List<MessageId>
    get() = messages.map(Message<*>::id)

val ProtoMessageGroup.messageIds: List<MessageID>
    get() = messagesList.map { message ->
        when (val kind = message.kindCase) {
            KindCase.MESSAGE -> message.message.metadata.id
            KindCase.RAW_MESSAGE -> message.rawMessage.metadata.id
            else -> error("Unknown message kind: $kind")
        }
    }