package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.*
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.infra.grpc.RawMessage
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext

class EncodeMessageSender(
    context: CoroutineContext,
    codecChannel: Channel<Deferred<RawMessageBatch>>,
    consumers: List<FilterChannelSender>
) : MessageSender<RawMessageBatch>(context, codecChannel, consumers) {

    override fun toCommonBatch(codecResult: RawMessageBatch): CommonBatch  = EncodeCommonBatch(codecResult)

    class EncodeCommonBatch(private val delegateBatch: RawMessageBatch) : CommonBatch {
        override fun filterNested(filter: Filter): CommonBatch {
            val filteredMessages = delegateBatch.messagesList.filter {
                filter.filter(toFilterInput(it))
            }
            return EncodeCommonBatch(RawMessageBatch.newBuilder().addAllMessages(filteredMessages).build())
        }

        override fun isEmpty(): Boolean = delegateBatch.messagesCount == 0

        override fun toByteArray(): ByteArray = delegateBatch.toByteArray()

        override fun toDebugString(): String {
            return delegateBatch.toDebugString()
        }

        private fun toFilterInput(message: RawMessage): FilterInput = FilterInput(
            MessageMetadata(
                message.metadata.id,
                message.metadata.timestamp,
                null
            ),
            null
        )
    }
}