package com.exactpro.th2.codec

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows

internal class TestDecodeProcessor {
    private val settings = mock<IExternalCodecSettings> { }
    private val codec = mock<IExternalCodec> {}
    private val factory = mock<IExternalCodecFactory> {
        on { createCodec(same(settings)) }.thenReturn(codec)
    }
    private val processor = DecodeProcessor(factory, settings, IMessageToProtoConverter())

    @Test
    internal fun `decodes one to one`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val messageID = MessageID.newBuilder()
            .setSequence(1)
            .build()
        val result = processor.process(RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }.build())

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

        val messageID = MessageID.newBuilder()
            .setSequence(1)
            .build()
        val result = processor.process(RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }.build())

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
    internal fun `throws exception if result's content does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = byteArrayOf(42, 44)
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val messageID = MessageID.newBuilder()
            .setSequence(1)
            .build()
        assertThrows<DecodeException> {
            processor.process(RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }.build())
        }
    }

    @Test
    internal fun `throws exception if result's content size does not match the original`() {
        val rawData = byteArrayOf(42, 43)
        val decodedMessage = DefaultMessageFactory.getFactory().createMessage("test", "test").apply {
            metaData.rawMessage = rawData + 44
        }

        whenever(codec.decode(any(), any())).thenReturn(listOf(decodedMessage))

        val messageID = MessageID.newBuilder()
            .setSequence(1)
            .build()
        assertThrows<DecodeException> {
            processor.process(RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).apply { metadataBuilder.id = messageID }.build())
        }
    }
}