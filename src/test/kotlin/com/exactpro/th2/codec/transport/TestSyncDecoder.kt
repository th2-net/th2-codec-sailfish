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
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

internal class TestSyncTransportDecoder {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private val router = mock<MessageRouter<GroupBatch>> { }
    private val applicationContext = mock<ApplicationContext> { }

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

    private val protoDecoder =
        TransportDecoder(router, applicationContext, "sourceAttributes", "targetAttributes", processor)

    @Test
    internal fun `decode protocol`() {
        val rawData = byteArrayOf(42, 43)

        whenever(codec.decode(any(), any())).thenAnswer { listOf(createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }) }

        protoDecoder.handle(DeliveryMetadata("tag"), GroupBatch.newMutable().apply {
            groups.add(createMessageGroup(RawMessage.newMutable(), rawData, 1)) // empty protocol
            groups.add(createMessageGroup(RawMessage.newMutable(), rawData, 2, factory.protocolName)) // codec protocol
            groups.add(createMessageGroup(RawMessage.newMutable(), rawData, 3, "test")) // another protocol
        })

        val captor = argumentCaptor<GroupBatch>()
        verify(router).sendAll(captor.capture(), any())

        val result: GroupBatch = captor.lastValue

        assertEquals(3, result.groups.size) { "Groups count: $result" }

        (result.groups.first()).let { group ->
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages.first()
            assertAll(
                { assertTrue(message is ParsedMessage) { "Type of first: $message" } },
                { assertEquals(1, message.id.sequence) { "Seq of first: $message" } }
            )
        }
        (result.groups[1]).let { group ->
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages.first()
            assertAll(
                { assertTrue(message is ParsedMessage) { "Type of second: $message" } },
                { assertEquals(2, message.id.sequence) { "Seq of second: $message" } }
            )
        }
        (result.groups[2]).let { group ->
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages.first()
            assertAll(
                { assertTrue(message is RawMessage) { "Type of third: $message" } },
                { assertEquals(3, message.id.sequence) { "Seq of third: $message" } }
            )
        }
    }

    private fun createMessageGroup(
        rawMessageBuilder: RawMessage,
        body: ByteArray,
        sequence: Long,
        protocol: String? = null
    ) = MessageGroup.newMutable().apply {
        messages.add(rawMessageBuilder.apply {
            if (protocol != null) {
                this.protocol = protocol
            }
            id.sequence = sequence
            this.body = Unpooled.wrappedBuffer(body)
        })
    }
}