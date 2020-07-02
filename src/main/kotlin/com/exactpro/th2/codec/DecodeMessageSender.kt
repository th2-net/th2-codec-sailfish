package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.*
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext

class DecodeMessageSender(
    context: CoroutineContext,
    codecChannel: Channel<Deferred<MessageBatch>>,
    consumers: List<FilterChannelSender>
) : MessageSender<MessageBatch>(context, codecChannel, consumers) {
    override fun toCommonBatch(codecResult: MessageBatch): CommonBatch  = DecodeCommonBatch(codecResult)
}