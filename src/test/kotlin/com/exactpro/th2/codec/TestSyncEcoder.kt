/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Message.Builder
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.TextFormat.shortDebugString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

internal class TestSyncEcoder {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private val router = mock<MessageRouter<MessageGroupBatch>> { }
    private val applicationContext = mock<ApplicationContext> { }
    private val converter = mock<ProtoToIMessageConverter> { }

    private val processor = EncodeProcessor(factory, settings, converter, mock {})
    private val encoder = SyncEncoder(router, applicationContext, processor)

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

        encoder.handler("tag", MessageGroupBatch.newBuilder().apply {
            addGroups(createAnyMessage(messageBuilder, 1)) // empty protocol
            addGroups(createAnyMessage(messageBuilder, 2, factory.protocolName)) // codec protocol
            addGroups(createAnyMessage(messageBuilder, 3, "test")) // another protocol
        }.build())

        val captor = argumentCaptor<MessageGroupBatch>()
        verify(router).sendAll(captor.capture(), any())

        val result = captor.lastValue

        Assertions.assertEquals(3, result.groupsCount) { "Groups count: ${shortDebugString(result)}" }

        (result.getGroups(0)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.RAW_MESSAGE, message.kindCase) { "Type of first: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(KindCase.RAW_MESSAGE, message.kindCase) { "Type of first: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(1, message.rawMessage.metadata.id.sequence) { "Seq of first: ${shortDebugString(message)}" } }
            )
        }
        (result.getGroups(1)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.RAW_MESSAGE, message.kindCase) { "Type of second: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(2, message.rawMessage.metadata.id.sequence) { "Seq of second: ${shortDebugString(message)}" } }
            )
        }
        (result.getGroups(2)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.MESSAGE, message.kindCase) { "Type of third: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(3, message.sequence) { "Seq of third: ${shortDebugString(message)}" } }
            )
        }
    }

    private fun createAnyMessage(messageBuilder: Builder, sequence: Long, protocol: String? = null) = MessageGroup.newBuilder()
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