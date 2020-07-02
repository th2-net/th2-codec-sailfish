package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.Filter
import com.exactpro.th2.codec.filter.FilterInput
import com.exactpro.th2.codec.filter.MessageMetadata
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch

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