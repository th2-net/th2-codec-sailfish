/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.Value

const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"

private fun Direction.toRole(): Role = when (this) {
    Direction.FIRST -> Role.RECEIVER
    Direction.SECOND -> Role.SENDER
    else -> throw IllegalStateException("Unsupported direction: $this")
}

private fun Role.toContext(properties: Map<String, Any> = emptyMap()): IExternalCodecContext =
    ExternalCodecContext(this, properties)

fun RawMessage.toCodecContext(): IExternalCodecContext {
    val properties = mapOf(
        MESSAGE_PROPERTIES.propertyName to metadata.propertiesMap
    )
    return metadata.id.direction.toRole().toContext(properties)
}

fun Message.toCodecContext(): IExternalCodecContext {
    val properties = mapOf(
        MESSAGE_PROPERTIES.propertyName to metadata.propertiesMap
    )
    return metadata.id.direction.toRole().toContext(properties)
}

fun RawMessage.toMessageMetadataBuilder(protocol: String): MessageMetadata.Builder {
    return MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setTimestamp(metadata.timestamp)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}

fun RawMessage.toErrorMessage(exception: Exception, protocol: String): Message.Builder = Message.newBuilder().apply {
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