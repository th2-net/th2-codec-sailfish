/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.filter.Filter
import com.exactpro.th2.codec.filter.FilterInput
import com.exactpro.th2.codec.filter.MessageMetadata
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch

class DecodeCommonBatch(private val delegateBatch: MessageBatch) : CommonBatch {
    override fun filterNested(filter: Filter): CommonBatch {
        val filteredMessages = delegateBatch.messagesList.filter {
            filter.filter(toFilterInput(it))
        }
        return DecodeCommonBatch(MessageBatch.newBuilder().addAllMessages(filteredMessages).build())
    }

    override fun isEmpty(): Boolean = delegateBatch.messagesCount == 0

    override fun toByteArray(): ByteArray = delegateBatch.toByteArray()

    override fun toDebugString(): String {
        return delegateBatch.toDebugString()
    }

    private fun toFilterInput(message: Message): FilterInput = FilterInput(
        MessageMetadata(
            message.metadata.id,
            message.metadata.timestamp,
            message.metadata.messageType
        ),
        message
    )
}