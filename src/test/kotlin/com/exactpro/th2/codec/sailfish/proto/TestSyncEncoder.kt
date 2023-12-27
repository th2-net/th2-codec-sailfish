/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.sailfish.proto

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Message.Builder
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.TextFormat.shortDebugString
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
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

    private val converter = mock<ProtoToIMessageConverter> { }

    private val processor = ProtoEncodeProcessor(factory, settings, converter)
    private val protoEncoder = ProtoEncoder(processor)

    private val context = mock<IReportingContext> {  }

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(context)
    }

    @Test
    internal fun `encode protocol`() {
        val rawData = byteArrayOf(42, 43)

        whenever(converter.fromProtoMessage(any<Message>(), any())).thenReturn(mock { })
        whenever(codec.encode(any(), any())).thenReturn(rawData)

        val messageBuilder = Message.newBuilder().apply {
            metadataBuilder.apply {
                id = MessageID.newBuilder().setSequence(1).build()
            }
        }

        protoEncoder.process(createAnyMessage(messageBuilder, 1), context).let { group -> // empty protocol
            assertNotNull(group)
            assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                {
                    assertEquals(KindCase.RAW_MESSAGE, message.kindCase) {
                        "Type of first: ${
                            shortDebugString(
                                message
                            )
                        }"
                    }
                },
                {
                    assertEquals(
                        1,
                        message.rawMessage.metadata.id.sequence
                    ) { "Seq of first: ${shortDebugString(message)}" }
                }
            )
        }
        protoEncoder.process(createAnyMessage(messageBuilder, 2, factory.protocolName), context)
            .let { group ->  // codec protocol
            assertNotNull(group)
            assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                {
                    assertEquals(KindCase.RAW_MESSAGE, message.kindCase) {
                        "Type of second: ${
                            shortDebugString(
                                message
                            )
                        }"
                    }
                },
                {
                    assertEquals(
                        2,
                        message.rawMessage.metadata.id.sequence
                    ) { "Seq of second: ${shortDebugString(message)}" }
                }
            )
        }
        protoEncoder.process(createAnyMessage(messageBuilder, 3, "test"), context).let { group ->  // another protocol
            assertNotNull(group)
            assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                {
                    assertEquals(KindCase.MESSAGE, message.kindCase) {
                        "Type of third: ${
                            shortDebugString(
                                message
                            )
                        }"
                    }
                },
                { assertEquals(3, message.sequence) { "Seq of third: ${shortDebugString(message)}" } }
            )
        }
    }

    private fun createAnyMessage(messageBuilder: Builder, sequence: Long, protocol: String? = null) =
        MessageGroup.newBuilder()
            .addMessages(
                AnyMessage.newBuilder().apply {
                    message =
                        messageBuilder.apply {
                            metadataBuilder.apply {
                                if (protocol != null) {
                                    this.protocol = protocol
                                }
                                idBuilder.apply {
                                    this.sequence = sequence
                                }
                            }
                        }.build()
                }.build()
            ).build()
}