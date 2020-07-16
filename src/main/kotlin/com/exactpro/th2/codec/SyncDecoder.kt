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
import com.exactpro.th2.codec.filter.DefaultFilterFactory
import com.exactpro.th2.codec.filter.FilterChannelSender
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.configuration.RabbitMQConfiguration
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import java.io.IOException
import java.lang.RuntimeException
import java.util.concurrent.TimeoutException

class SyncDecoder(
    codecParameters: CodecParameters,
    applicationContext: ApplicationContext
): AutoCloseable {

    private val logger = KotlinLogging.logger {}
    private val processor: DecodeProcessor = DecodeProcessor(
        applicationContext.codecFactory,
        applicationContext.codecSettings,
        applicationContext.messageToProtoConverter
    )
    private val consumers: List<FilterChannelSender>
    private val subscriber: RabbitMqSubscriber
    private val rabbitMQConnection: Connection

    init {
        subscriber = RabbitMqSubscriber(
            codecParameters.inParams.exchangeName,
            DeliverCallback(this::handle),
            null, // FIXME handle cancellation
            codecParameters.inParams.queueName
        )
        rabbitMQConnection = applicationContext.connectionFactory.newConnection()
        val channel = rabbitMQConnection.createChannel()
        channel.exchangeDeclare(codecParameters.inParams.exchangeName, "direct")
        consumers = codecParameters.outParams.filters.map {
            val filter = DefaultFilterFactory().create(it)
            logger.info { "decode out created with queue '${it.queueName}' and filter '${it.filterType}'" }
            FilterChannelSender(channel, filter, it.exchangeName, it.queueName)
        }
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
        close(rabbitMQConnection, "senderMqConnection", exceptions)
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

    private fun handle(consumerTag: String, message: Delivery) {
        val protoSource = toProtoMessage(message)
        val parsedBatch = processor.process(protoSource)
        if (parsedBatch.messagesCount !=  0) {
            for (consumer in consumers) {
                consumer.filterAndSend(DecodeCommonBatch(parsedBatch))
            }
        }
    }

    private fun toProtoMessage(message: Delivery): RawMessageBatch {
        val protoMessage = RawMessageBatch.parseFrom(message.body)
        logger.debug {
            val debugMessage = protoMessage.toDebugString()
            "received message from '${message.envelope.exchange}':'${message.envelope.routingKey}': $debugMessage"
        }
        return protoMessage
    }
}