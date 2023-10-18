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
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
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
    private val converter = mock<TransportToIMessageConverter> { }

    private val processor = TransportEncodeProcessor(factory, settings, converter)
    private val transportEncoder =
        TransportEncoder(router, applicationContext, "sourceAttributes", "targetAttributes", processor)

    @Test
    internal fun `encode protocol`() {
        val rawData = byteArrayOf(42, 43)

        whenever(converter.fromTransport(any(), any(), any(), any())).thenReturn(mock { })
        whenever(codec.encode(any(), any())).thenReturn(rawData)

        transportEncoder.handle(DeliveryMetadata("tag"), GroupBatch.builder().apply {
            addGroup(createMessageGroup(ParsedMessage.builder(), 1)) // empty protocol
            addGroup(createMessageGroup(ParsedMessage.builder(), 2, factory.protocolName)) // codec protocol
            addGroup(createMessageGroup(ParsedMessage.builder(), 3, "test")) // another protocol
            setSessionGroup("group")
            setBook("book")
        }.build())

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

    private fun createMessageGroup(messageBuilder: ParsedMessage.FromMapBuilder, sequence: Long, protocol: String? = null) =
        MessageGroup.builder().apply {
            addMessage(
                messageBuilder.apply {
                    if (protocol != null) {
                        setProtocol(protocol)
                    }
                    idBuilder().setSequence(sequence)
                    setType("type")
                    setBody(emptyMap())
                }.build()
            )
        }.build()
}