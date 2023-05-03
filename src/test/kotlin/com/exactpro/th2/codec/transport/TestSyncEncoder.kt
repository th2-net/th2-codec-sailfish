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

import com.exactpro.sf.common.messages.MsgMetaData
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

internal class TestSyncEncoder {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private val router = mock<MessageRouter<GroupBatch>> { }
    private val applicationContext = mock<ApplicationContext> { }
    private val messageFactory = mock<TransportIMessageFactory> {
        on { createMessage(any<ParsedMessage>()) }.thenAnswer {
            val builder = it.arguments[0] as ParsedMessage
            TransportIMessage(
                message = builder,
                metadata = MsgMetaData("mock", builder.type),
                toTransportConverter = mock { },
                fromTransportConverter = mock { },
                messageFactory = it.mock as TransportIMessageFactory,
                msgStructure = mock { }
            )
        }
    }

    private val processor = TransportEncodeProcessor(factory, settings, messageFactory)
    private val transportEncoder =
        TransportEncoder(router, applicationContext, "sourceAttributes", "targetAttributes", processor)

    @Test
    internal fun `encode protocol`() {
        val rawData = byteArrayOf(42, 43)

        whenever(messageFactory.createMessage(any<MsgMetaData>())).thenReturn(mock { })
        whenever(codec.encode(any(), any())).thenReturn(rawData)

        transportEncoder.handle(DeliveryMetadata("tag"), GroupBatch.newMutable().apply {
            groups.add(createMessageGroup(ParsedMessage.newMutable(), 1)) // empty protocol
            groups.add(createMessageGroup(ParsedMessage.newMutable(), 2, factory.protocolName)) // codec protocol
            groups.add(createMessageGroup(ParsedMessage.newMutable(), 3, "test")) // another protocol
        })

        val captor = argumentCaptor<GroupBatch>()
        verify(router).sendAll(captor.capture(), any())

        val result = captor.lastValue

        Assertions.assertEquals(3, result.groups.size) { "Groups count: $result" }

        (result.groups[0]).let { group ->
            Assertions.assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { Assertions.assertTrue(message is RawMessage) { "Type of first: $message" } },
                { Assertions.assertEquals(1, message.id.sequence) { "Seq of first: $message" } }
            )
        }
        (result.groups[1]).let { group ->
            Assertions.assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { Assertions.assertTrue(message is RawMessage) { "Type of second: $message" } },
                { Assertions.assertEquals(2, message.id.sequence) { "Seq of second: $message" } }
            )
        }
        (result.groups[2]).let { group ->
            Assertions.assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { Assertions.assertTrue(message is ParsedMessage) { "Type of third: $message" } },
                { Assertions.assertEquals(3, message.id.sequence) { "Seq of third: $message" } }
            )
        }
    }

    private fun createMessageGroup(messageBuilder: ParsedMessage, sequence: Long, protocol: String? = null) =
        MessageGroup.newMutable().apply {
            messages.add(
                messageBuilder.apply {
                    if (protocol != null) {
                        this.protocol = protocol
                    }
                    id.sequence = sequence
                }
            )
        }
}