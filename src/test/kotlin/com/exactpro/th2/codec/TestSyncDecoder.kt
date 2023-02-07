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
import com.exactpro.th2.common.schema.message.DeliveryMetadata
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

        decoder.handle(DeliveryMetadata("tag"), MessageGroupBatch.newBuilder().apply {
            addGroups(createAnyMessage(rawMessageBuilder, 1)) // empty protocol
            addGroups(createAnyMessage(rawMessageBuilder, 2, factory.protocolName)) // codec protocol
            addGroups(createAnyMessage(rawMessageBuilder, 3, "test")) // another protocol
        }.build())

        val captor = argumentCaptor<MessageGroupBatch>()
        verify(router).sendAll(captor.capture(), any())

        val result: MessageGroupBatch = captor.lastValue

        Assertions.assertEquals(3, result.groupsCount) { "Groups count: ${shortDebugString(result)}" }

        (result.getGroups(0)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.MESSAGE, message.kindCase) { "Type of first: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(1, message.message.sequence) { "Seq of first: ${shortDebugString(message)}" } }
            )
        }
        (result.getGroups(1)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.MESSAGE, message.kindCase) { "Type of second: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(2, message.message.sequence) { "Seq of second: ${shortDebugString(message)}" } }
            )
        }
        (result.getGroups(2)).let { group ->
            Assertions.assertEquals(1, group.messagesCount) { "Messages count: ${shortDebugString(group)}" }
            val message = group.getMessages(0)
            assertAll(
                { Assertions.assertEquals(KindCase.RAW_MESSAGE, message.kindCase) { "Type of third: ${shortDebugString(message)}" } },
                { Assertions.assertEquals(3, message.rawMessage.metadata.id.sequence) { "Seq of third: ${shortDebugString(message)}" } }
            )
        }
    }

    private fun createAnyMessage(rawMessageBuilder: Builder, sequence: Long, protocol: String? = null) = MessageGroup.newBuilder()
        .addMessages(
            AnyMessage.newBuilder().apply {
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
        ).build()
}