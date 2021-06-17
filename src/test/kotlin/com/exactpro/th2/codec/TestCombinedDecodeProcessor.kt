/*
 *  Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.inOrder
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestCombinedDecodeProcessor {
    private val codec = mock<IExternalCodec> { }
    private val codecSettings = mock<IExternalCodecSettings> { }
    private val codecFactory = mock<IExternalCodecFactory> {
        on { createCodec(same(codecSettings)) }.thenReturn(codec)
    }
    private val processor = CombinedDecodeProcessor(codecFactory, codecSettings, IMessageToProtoConverter())

    @Test
    fun `correctly splits the batch`() {
        val firstRaw1 = byteArrayOf(42)
        val firstRaw2 = byteArrayOf(43)
        val secondRaw1 = byteArrayOf(44, 45)
        val secondRaw2 = byteArrayOf(46)
        val firstRaw3 = byteArrayOf(47)
        val batch = RawMessageBatch.newBuilder()
            .addMessages(createRawMessage(firstRaw1, Direction.FIRST, 1))
            .addMessages(createRawMessage(firstRaw2, Direction.FIRST, 2))
            .addMessages(createRawMessage(secondRaw1, Direction.SECOND, 1)) // Produces 2 messages: Header + Body
            .addMessages(createRawMessage(secondRaw2, Direction.SECOND, 2)) // Produces 1 message: Header
            .addMessages(createRawMessage(firstRaw3, Direction.FIRST, 3))
            .build()

        whenever(codec.decode(any(), any())).then {
            val raw = it.arguments[0] as ByteArray
            listOf(message("Header", raw, true))
        }

        whenever(codec.decode(eq(byteArrayOf(42, 43)), any())).thenReturn(
            listOf(
                message("Header", byteArrayOf(42), true),
                message("Body", byteArrayOf(43))
            )
        )

        whenever(codec.decode(eq(byteArrayOf(44, 45)), any())).thenReturn(
            listOf(
                message("Header", byteArrayOf(44), true),
                message("Body", byteArrayOf(45))
            )
        )

        val result = processor.process(batch)
        Assertions.assertEquals(batch.messagesList.map { it.metadata.id }, result.messagesList.map { it.metadata.id })
        Assertions.assertEquals(listOf("Header", "Body", "Body", "Header", "Header"), result.messagesList.map { it.metadata.messageType })
        inOrder(codec) {
            verify(codec).decode(eq(byteArrayOf(42, 43)), any()) // first1 + first2
            verify(codec).decode(eq(byteArrayOf(44, 45)), any()) // second1
            verify(codec).decode(eq(byteArrayOf(46)), any()) // second2
            verify(codec).decode(eq(byteArrayOf(47)), any()) // first3
            verifyNoMoreInteractions()
        }
    }

    private fun createRawMessage(raw: ByteArray, direction: Direction, seq: Long) = RawMessage.newBuilder()
        .setBody(ByteString.copyFrom(raw))
        .apply {
            metadataBuilder.idBuilder.apply {
                setDirection(direction)
                setSequence(seq)
            }
        }
        .build()

    private fun message(name: String, raw: ByteArray, admin: Boolean = false): IMessage {
        return DefaultMessageFactory.getFactory().createMessage(name, "test").apply {
            metaData.rawMessage = raw
            metaData.isAdmin = admin
        }
    }
}