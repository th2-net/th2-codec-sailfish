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

package com.exactpro.th2.codec.proto

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.google.protobuf.ByteString
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.whenever
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.mockito.Mockito.`when`

internal class TestDecodeProcessor {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
        on { protocolName }.thenReturn("mock")
    }
    private val commonFactory = mock<CommonFactory> {
        `when`(it.newMessageIDBuilder()).thenCallRealMethod()
        `when`(it.boxConfiguration).thenReturn(BoxConfiguration())
    }
    private val processor = ProtoDecodeProcessor(
        factory,
        settings,
        IMessageToProtoConverter(),
        mock {},
    )

    @Test
    internal fun `decodes one to one`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val messageID = commonFactory.newMessageIDBuilder()
            .setSequence(1)
            .build()
        val result = processor.process(
            MessageGroupBatch.getDefaultInstance(),
            RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }
                .build()
        )

        assertEquals(1, result.size) { "Unexpected result: $result" }
        val id = result[0].metadata.id
        assertEquals(messageID, id) { "Unexpected message id: $id" }
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

        val messageID = commonFactory.newMessageIDBuilder()
            .setSequence(1)
            .build()
        val result = processor.process(
            MessageGroupBatch.getDefaultInstance(),
            RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }
                .build()
        )

        assertEquals(2, result.size) { "Unexpected result: $result" }
        assertAll(
            result.map {
                {
                    val id = it.metadata.id
                    assertEquals(messageID, id) { "Unexpected message id: $id" }
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

        val messageID = commonFactory.newMessageIDBuilder()
            .setSequence(1)
            .build()

        val builder = processor.process(
            MessageGroupBatch.getDefaultInstance(),
            RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }
                .build()
        )[0]
        assertEquals(ERROR_TYPE_MESSAGE, builder.metadata.messageType)
    }

    @Test
    internal fun `creates an error message if there was DecodeException`() {
        whenever(codec.decode(any(), any())).thenThrow(DecodeException("Test"))

        val messageID = commonFactory.newMessageIDBuilder()
            .setSequence(1)
            .build()

        val builder = processor.process(
            MessageGroupBatch.getDefaultInstance(),
            RawMessage.newBuilder().setBody(ByteString.copyFrom(byteArrayOf(42, 43)))
                .apply { metadataBuilder.id = messageID }.build()
        )[0]
        assertEquals(ERROR_TYPE_MESSAGE, builder.metadata.messageType)
        assertEquals("Caused by: Test. ", builder.getField(ERROR_CONTENT_FIELD)?.simpleValue)
    }


    @Test
    internal fun `creates an error message if result's content size does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData + 44
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val messageID = commonFactory.newMessageIDBuilder()
            .setSequence(1)
            .build()

        val builder = processor.process(
            MessageGroupBatch.getDefaultInstance(),
            RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }
                .build()
        )[0]
        assertEquals(ERROR_TYPE_MESSAGE, builder.metadata.messageType)
        assertNotNull(builder.getField(ERROR_CONTENT_FIELD))
    }
}