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

import com.exactpro.th2.RabbitMqSubscriber
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.CodecParameters
import com.exactpro.th2.codec.filter.AnyFilter
import com.exactpro.th2.codec.filter.DefaultFilterFactory
import com.exactpro.th2.codec.filter.FilterChannelSender
import com.exactpro.th2.configuration.RabbitMQConfiguration
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.rabbitmq.client.Connection
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.newSingleThreadContext
import mu.KotlinLogging
import java.io.IOException
import java.lang.Runtime.getRuntime
import java.lang.RuntimeException
import java.util.concurrent.TimeoutException

@ObsoleteCoroutinesApi
class Decoder(codecParameters: CodecParameters, applicationContext: ApplicationContext) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val coroutineContext = newFixedThreadPoolContext(getRuntime().availableProcessors() * 2, "decoder-context")
    private val coroutineChannel: Channel<Deferred<MessageBatch>> = Channel(CHANNEL_SIZE)
    private val messageHandler: MessageHandler<RawMessageBatch, MessageBatch>
    private val subscriber: RabbitMqSubscriber
    private val messageSender: MessageSender<MessageBatch>
    private val rabbitMQConnection: Connection

    init {
        messageHandler = object : MessageHandler<RawMessageBatch, MessageBatch>(
            CumulativeDecodeProcessor(
                applicationContext.codecFactory,
                applicationContext.codecSettings,
                applicationContext.messageToProtoConverter
            ),
            coroutineContext,
            coroutineChannel
        ) {
            override fun toProtoMessage(byteArray: ByteArray): RawMessageBatch  = RawMessageBatch.parseFrom(byteArray)
        }
        subscriber = RabbitMqSubscriber(
                codecParameters.inParams.exchangeName,
                messageHandler,
                null, // FIXME handle cancellation
                codecParameters.inParams.queueName
        )
        rabbitMQConnection = applicationContext.connectionFactory.newConnection()
        val channel = rabbitMQConnection.createChannel()
        channel.exchangeDeclare(codecParameters.inParams.exchangeName, "direct")
        messageSender = DecodeMessageSender(
            newSingleThreadContext("sender-context"),
            coroutineChannel,
            codecParameters.outParams.filters.map {
                val filter = if (it.parameters != null) {
                    DefaultFilterFactory().create(it.parameters!!)
                } else {
                    AnyFilter()
                }
                logger.info { "decode out created with queue '${it.queueName}'" }
                FilterChannelSender(channel, filter, it.exchangeName, it.queueName)
            }
        )
    }

    fun start(rabbitMQParameters: RabbitMQConfiguration) {
        try {
            subscriber.startListening(
                rabbitMQParameters.host,
                rabbitMQParameters.virtualHost,
                rabbitMQParameters.port,
                rabbitMQParameters.username,
                rabbitMQParameters.password
            )
            messageSender.start()
        } catch (exception: Exception) {
            when(exception) {
                is IOException,
                is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {
        val exceptions = mutableListOf<Exception>()
        close(subscriber, "subscriber", exceptions)
        close(messageSender, "messageSender", exceptions)
        if (exceptions.isNotEmpty()) {
            throw RuntimeException("could not close decoder").also {
                exceptions.forEach { exception -> it.addSuppressed(exception) }
            }
        }
    }

    private fun close(closeable: AutoCloseable, name: String, exceptions: MutableList<Exception>) {
        try {
            closeable.close()
        } catch (exception: Exception) {
            exceptions.add(RuntimeException("could not close '$name'. Reason: ${exception.message}", exception))
        }
    }

    companion object {
        const val CHANNEL_SIZE = 10000
    }
}