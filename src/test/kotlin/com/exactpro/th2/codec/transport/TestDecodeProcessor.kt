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

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.MsgMetaData
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.proto.ProtoIMessage
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.whenever
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

internal class TestDecodeProcessor {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private fun createMessage(namespace: String, name: String): IMessage {
        return TransportIMessage(
            message = ParsedMessage.newMutable(),
            metadata = MsgMetaData(namespace, name),
            toTransportConverter = mock { },
            fromTransportConverter = mock { },
            messageFactory = mock {  },
            msgStructure = mock { }
        )
    }

    private val processor = TransportDecodeProcessor(
        factory,
        settings,
        mock {  },
        mock {},
    )

    @Test
    internal fun `decodes one to one`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.newMutable().apply {
            id.sequence = 1
            body = Unpooled.wrappedBuffer(rawData)
        }
        val batch = GroupBatch.newMutable().apply {
            groups.add(MessageGroup.newMutable().apply {
                messages.add(rawMessage)
            })
        }
        val result = processor.process(batch, rawMessage)

        assertEquals(1, result.size) { "Unexpected result: $result" }
        val id = result[0].id
        assertEquals(rawMessage.id, id) { "Unexpected message id: $id" }
    }

    @Test
    internal fun `decodes one to many`() {
        val rawData = byteArrayOf(42, 43, 44, 45)
        val decodedMessage1 = createMessage("test1", "test").apply {
            metaData.rawMessage = byteArrayOf(42, 43)
        }
        val decodedMessage2 = createMessage("test1", "test").apply {
            metaData.rawMessage = byteArrayOf(44, 45)
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage1, decodedMessage2))

        val rawMessage = RawMessage.newMutable().apply {
            id.sequence = 1
            body = Unpooled.wrappedBuffer(rawData)
        }
        val batch = GroupBatch.newMutable().apply {
            groups.add(MessageGroup.newMutable().apply {
                messages.add(rawMessage)
            })
        }
        val result = processor.process(batch, rawMessage)

        assertEquals(2, result.size) { "Unexpected result: $result" }
        assertAll(
            result.map {
                {
                    val id = it.id
                    assertEquals(rawMessage.id, id) { "Unexpected message id: $id" }
                }
            }
        )
    }

    @Test
    internal fun `creates an error message if result's content does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = createMessage("test", "test").apply {
            metaData.rawMessage = byteArrayOf(42, 44)
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.newMutable().apply {
            id.sequence = 1
            body = Unpooled.wrappedBuffer(rawData)
        }
        val batch = GroupBatch.newMutable().apply {
            groups.add(MessageGroup.newMutable().apply {
                messages.add(rawMessage)
            })
        }
        val result = processor.process(batch, rawMessage)[0]
        assertEquals(ERROR_TYPE_MESSAGE, result.type)
    }

    @Test
    internal fun `creates an error message if there was DecodeException`() {
        val rawData = byteArrayOf(42, 43)
        whenever(codec.decode(any(), any())).thenThrow(DecodeException("Test"))

        val rawMessage = RawMessage.newMutable().apply {
            id.sequence = 1
            body = Unpooled.wrappedBuffer(rawData)
        }
        val batch = GroupBatch.newMutable().apply {
            groups.add(MessageGroup.newMutable().apply {
                messages.add(rawMessage)
            })
        }
        val firstMsg = processor.process(batch, rawMessage)[0]
        assertEquals(ERROR_TYPE_MESSAGE, firstMsg.type)
        assertEquals("Caused by: Test. ", firstMsg.body[ERROR_CONTENT_FIELD])
    }


    @Test
    internal fun `creates an error message if result's content size does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = createMessage("test", "test").apply {
            metaData.rawMessage = rawData + 44
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.newMutable().apply {
            id.sequence = 1
            body = Unpooled.wrappedBuffer(rawData)
        }
        val batch = GroupBatch.newMutable().apply {
            groups.add(MessageGroup.newMutable().apply {
                messages.add(rawMessage)
            })
        }
        val result = processor.process(batch, rawMessage)[0]
        assertEquals(ERROR_TYPE_MESSAGE, result.type)
        assertNotNull(result.body[ERROR_CONTENT_FIELD])
    }
}