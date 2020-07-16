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

import com.exactpro.th2.codec.util.toDebugString
import com.google.protobuf.GeneratedMessageV3
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext

abstract class MessageHandler<T : GeneratedMessageV3, R>(
    private val processor: MessageProcessor<T, R>,
    private val context: CoroutineContext,
    private val channel: Channel<Deferred<R>>
): DeliverCallback {

    private val logger = KotlinLogging.logger {}
    private val customScope: CoroutineScope = CoroutineScope(context)

    override fun handle(consumerTag: String, message: Delivery) {
        runBlocking {
            val deferred = customScope.async {
                val protoSource = parse(message)
                processor.process(protoSource)
            }
            channel.send(deferred)
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