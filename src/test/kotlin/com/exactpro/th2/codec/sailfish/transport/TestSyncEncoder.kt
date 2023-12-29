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

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import kotlin.test.assertNotNull

internal class TestSyncEncoder {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private val converter = mock<TransportToIMessageConverter> { }

    private val processor = TransportEncodeProcessor(factory, settings, converter)
    private val transportEncoder = TransportEncoder(processor)

    private val context = mock<IReportingContext> {  }

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(context)
    }

    @Test
    internal fun `encode protocol`() {
        val rawData = byteArrayOf(42, 43)

        whenever(converter.fromTransport(any(), any(), any(), any())).thenReturn(mock { })
        whenever(codec.encode(any(), any())).thenReturn(rawData)

        transportEncoder.process(createMessageGroup(ParsedMessage.builder(), 1), context)
            .let { group ->  // empty protocol
            assertNotNull(group)
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { assertTrue(message is RawMessage) { "Type of first: $message" } },
                { assertEquals(1, message.id.sequence) { "Seq of first: $message" } }
            )
        }
        transportEncoder.process(
            createMessageGroup(ParsedMessage.builder(), 2, factory.protocolName),
            context
        ).let { group -> // codec protocol
            assertNotNull(group)
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { assertTrue(message is RawMessage) { "Type of second: $message" } },
                { assertEquals(2, message.id.sequence) { "Seq of second: $message" } }
            )
        }
        transportEncoder.process(createMessageGroup(ParsedMessage.builder(), 3, "test"), context)
            .let { group -> // another protocol
            assertNotNull(group)
            assertEquals(1, group.messages.size) { "Messages count: $group" }
            val message = group.messages[0]
            assertAll(
                { assertTrue(message is ParsedMessage) { "Type of third: $message" } },
                { assertEquals(3, message.id.sequence) { "Seq of third: $message" } }
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