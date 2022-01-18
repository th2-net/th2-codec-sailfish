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

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessage.Builder
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.google.protobuf.ByteString
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

internal class TestSyncDecoder {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }

    private val router = mock<MessageRouter<MessageGroupBatch>> { }
    private val applicationContext = mock<ApplicationContext> { }

    private val processor = DecodeProcessor(factory, settings, IMessageToProtoConverter(), mock {})
    private val decoder = SyncDecoder(router, applicationContext, processor)

    @Test
    internal fun `decode protocol`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val rawMessageBuilder = RawMessage.newBuilder().apply {
            body = ByteString.copyFrom(rawData)
            metadataBuilder.apply {
                id = MessageID.newBuilder().setSequence(1).build()
            }
        }

        decoder.handler("tag", MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                addMessages(createAnyMessage(rawMessageBuilder, 1)) // empty protocol
                addMessages(createAnyMessage(rawMessageBuilder, 2, factory.protocolName)) // codec protocol
                addMessages(createAnyMessage(rawMessageBuilder, 3, "test")) // another protocol
            })
        }.build())

        val captor = argumentCaptor<MessageGroupBatch>()
        verify(router).sendAll(captor.capture(), any())

        val result = captor.lastValue

        Assertions.assertEquals(1, result.groupsCount) { "Groups count: ${shortDebugString(result)}" }
        val group = result.getGroups(0)
        Assertions.assertEquals(3, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }

        val first = group.getMessages(0)
        assertAll(
            { Assertions.assertEquals(KindCase.MESSAGE, first.kindCase) { "Type of first: ${shortDebugString(first)}" } },
            { Assertions.assertEquals(1, first.message.sequence) { "Seq of first: ${shortDebugString(first)}" } }
        )
        val second = group.getMessages(1)
        assertAll(
            { Assertions.assertEquals(KindCase.MESSAGE, second.kindCase) { "Type of second: ${shortDebugString(first)}" } },
            { Assertions.assertEquals(2, second.message.sequence) { "Seq of second: ${shortDebugString(first)}" } }
        )
        val third = group.getMessages(2)
        assertAll(
            { Assertions.assertEquals(KindCase.RAW_MESSAGE, third.kindCase) { "Type of third: ${shortDebugString(third)}" } },
            { Assertions.assertEquals(3, third.rawMessage.metadata.id.sequence) { "Seq of third: ${shortDebugString(third)}" } }
        )
    }

    private fun createAnyMessage(rawMessageBuilder: Builder, sequence: Long, protocol: String? = null) = AnyMessage.newBuilder().apply {
        rawMessage =
            rawMessageBuilder.apply {
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
}