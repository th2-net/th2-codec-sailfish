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
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.configuration.RabbitMQConfiguration
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.eventstore.grpc.StoreEventRequest
import com.exactpro.th2.infra.grpc.Event
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.EventStatus
import com.google.protobuf.ByteString.copyFrom
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.InvalidProtocolBufferException
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec<T: GeneratedMessageV3, R: GeneratedMessageV3>(
    codecParameters: CodecParameters,
    applicationContext: ApplicationContext,
    protected val processor: AbstractCodecProcessor<T, R>,
    protected val codecRootEvent: EventID?
): AutoCloseable {

    protected val logger = KotlinLogging.logger {}
    protected val consumers: List<FilterChannelSender>
    protected val subscriber: RabbitMqSubscriber
    protected val rabbitMQConnection: Connection
    protected val eventStoreConnector: EventStoreServiceFutureStub? = applicationContext.eventConnector

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
            val filter = if (it.parameters != null) {
                DefaultFilterFactory().create(it.parameters!!)
            } else {
                AnyFilter()
            }
            logger.info { "decode out created with queue '${it.queueName}'" }
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
        var protoSource: T? = null
        var protoResult: R? = null
        try {
            protoSource = toProtoSource(message)
            protoResult = processor.process(protoSource)
            if (checkResult(protoResult)) {
                for (consumer in consumers) {
                    consumer.filterAndSend(toCommonBatch(protoResult))
                }
            }
        } catch (exception: CodecException) {
            val parentEventId = getParentEventId(codecRootEvent, protoSource, protoResult)
            if (parentEventId != null) {
                createAndStoreErrorEvent(exception, parentEventId)
            }
            logger.error(exception) {}
        }
    }

    private fun createAndStoreErrorEvent(exception: CodecException, parentEventID: EventID) {
        if (eventStoreConnector != null) {
            try {
                eventStoreConnector.storeEvent(
                    StoreEventRequest.newBuilder()
                        .setEvent(
                            Event.newBuilder()
                                .setName("Codec error")
                                .setType("CodecError")
                                .setStatus(EventStatus.FAILED)
                                .setParentId(parentEventID)
                                .setBody(copyFrom("{\"message\":\"${exception.getAllMessages()}\"}", UTF_8))
                                .build()
                        )
                        .build()
                )
            } catch (exception: Exception) {
                logger.warn(exception) { "could not send codec error event" }
            }
        }
    }

    private fun toProtoSource(message: Delivery): T {
        try {
            val protoMessage = parseProtoSourceFrom(message.body)
            logger.debug {
                val debugMessage = protoMessage.toDebugString()
                "received ${protoMessage::class.java.simpleName} from " +
                        "'${message.envelope.exchange}':'${message.envelope.routingKey}': $debugMessage"
            }
            return protoMessage
        } catch (exception: InvalidProtocolBufferException) {
            throw CodecException("'${message.envelope.exchange}':'${message.envelope.routingKey}': " +
                    "could not parse message body to proto", exception)
        }
    }

    abstract fun getParentEventId(codecRootID: EventID?, protoSource: T?, protoResult: R?): EventID?
    abstract fun parseProtoSourceFrom(data: ByteArray): T
    abstract fun checkResult(protoResult: R): Boolean
    abstract fun toCommonBatch(protoResult: R): CommonBatch
}