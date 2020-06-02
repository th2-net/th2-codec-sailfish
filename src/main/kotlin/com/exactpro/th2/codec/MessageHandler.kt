package com.exactpro.th2.codec

import com.exactpro.th2.codec.util.toDebugString
import com.google.protobuf.GeneratedMessageV3
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext

abstract class MessageHandler<T : GeneratedMessageV3, R>(
    private val processor: MessageProcessor<T, R>,
    private val context: CoroutineContext,
    private val channel: Channel<Deferred<R>>
): DeliverCallback {

    private val logger = KotlinLogging.logger {}

    override fun handle(consumerTag: String, message: Delivery) {
        runBlocking(context) {
            val deferred = async {
                val protoSource = parse(message)
                processor.process(protoSource)
            }
            channel.offer(deferred)
        }
    }

    abstract fun toProtoMessage(byteArray: ByteArray): T

    private fun parse(message: Delivery): T {
        val protoMessage = toProtoMessage(message.body)
        logger.debug {
            "received message from '${message.envelope.exchange}':'${message.envelope.routingKey}':" +
                    " ${protoMessage.toDebugString()}"
        }
        return protoMessage
    }
}