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
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.whenever
import io.netty.buffer.Unpooled
import java.time.Instant
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
    private val processor = TransportDecodeProcessor(
        factory,
        settings,
        IMessageToTransportConverter(),
        mock {},
    )

    @Test
    internal fun `decodes one to one`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.builder().apply {
            setProperties()
            setBody(Unpooled.wrappedBuffer(rawData))
        }.build()
        val batch = GroupBatch.builder().apply {
            addGroup(MessageGroup.builder().apply {
                addMessage(rawMessage)
                setProperties()
            }.build())
        }.build()
        val result = processor.process(batch, rawMessage)

        assertEquals(1, result.size) { "Unexpected result: $result" }
        val id = result[0].build().id
        assertEquals(rawMessage.id, id) { "Unexpected message id: $id" }
    }

    @Test
    internal fun `decodes one to many`() {
        val rawData = byteArrayOf(42, 43, 44, 45)
        val decodedMessage1 = DefaultMessageFactory.getFactory().createMessage("test1", "test").apply {
            metaData.rawMessage = byteArrayOf(42, 43)
        }
        val decodedMessage2 = DefaultMessageFactory.getFactory().createMessage("test1", "test").apply {
            metaData.rawMessage = byteArrayOf(44, 45)
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage1, decodedMessage2))

        val rawMessage = RawMessage.builder().apply {
            setProperties()
            setBody(Unpooled.wrappedBuffer(rawData))
        }.build()
        val batch = GroupBatch.builder().apply {
            addGroup(MessageGroup.builder().apply {
                addMessage(rawMessage)
                setProperties()
            }.build())
        }.build()
        val result = processor.process(batch, rawMessage)

        assertEquals(2, result.size) { "Unexpected result: $result" }
        assertAll(
            result.map { it.build() }.map {
                {
                    val id = it.id
                    println(id)
                    assertEquals(rawMessage.id, id) { "Unexpected message id: $id" }
                }
            }
        )
    }

    @Test
    internal fun `creates an error message if result's content does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = byteArrayOf(42, 44)
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.builder().apply {
            setProperties()
            setBody(Unpooled.wrappedBuffer(rawData))
        }.build()
        val batch = GroupBatch.builder().apply {
            addGroup(MessageGroup.builder().apply {
                addMessage(rawMessage)
                setProperties()
            }.build())
        }.build()
        val result = processor.process(batch, rawMessage)[0]
        assertEquals(ERROR_TYPE_MESSAGE, result.type)
    }

    @Test
    internal fun `creates an error message if there was DecodeException`() {
        val rawData = byteArrayOf(42, 43)
        whenever(codec.decode(any(), any())).thenThrow(DecodeException("Test"))

        val rawMessage = RawMessage.builder().apply {
            setProperties()
            setBody(Unpooled.wrappedBuffer(rawData))
        }.build()
        val batch = GroupBatch.builder().apply {
            addGroup(MessageGroup.builder().apply {
                setProperties()
                addMessage(rawMessage)
            }.build())
            setBook("book")
            setSessionGroup("sessionGroup")
        }.build()
        val firstMsg = processor.process(batch, rawMessage)[0].build()
        assertEquals(ERROR_TYPE_MESSAGE, firstMsg.type)
        assertEquals("Caused by: Test. ", firstMsg.body[ERROR_CONTENT_FIELD])
    }


    @Test
    internal fun `creates an error message if result's content size does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData + 44
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessage = RawMessage.builder().apply {
            setProperties()
            setBody(Unpooled.wrappedBuffer(rawData))
        }.build()
        val batch = GroupBatch.builder().apply {
            addGroup(MessageGroup.builder().apply {
                addMessage(rawMessage)
                setProperties()
            }.build())
        }.build()
        val result = processor.process(batch, rawMessage)[0].build()
        assertEquals(ERROR_TYPE_MESSAGE, result.type)
        assertNotNull(result.body[ERROR_CONTENT_FIELD])
    }

    fun GroupBatch.Builder.setProperties() {
        apply {
            setBook(BOOK)
            setSessionGroup(SESSION_GROUP)
        }
    }

    fun RawMessage.Builder.setProperties() {
        apply {
            idBuilder().apply {
                addSubsequence(1)
                setSequence(1)
                setDirection(Direction.OUTGOING)
                setTimestamp(Instant.now())
                setSessionAlias(SESSIONS_ALIAS)
            }
            setProtocol(PROTOCOL)
        }
    }

    companion object {
        private const val BOOK = "book"
        private const val SESSION_GROUP = "sessionGroup"
        private const val SESSIONS_ALIAS = "sessionAlias"
        private const val PROTOCOL = "protocol"
    }
}