package com.exactpro.th2.codec

import com.exactpro.th2.RabbitMqSubscriber
import com.exactpro.th2.codec.filter.DefaultFilterFactory
import com.exactpro.th2.codec.filter.FilterChannelSender
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
import java.util.concurrent.TimeoutException

@ObsoleteCoroutinesApi
class Decoder(configuration: Configuration, applicationContext: ApplicationContext) {
    private val logger = KotlinLogging.logger {}
    private val coroutineContext = newFixedThreadPoolContext(getRuntime().availableProcessors(), "decoder-context")
    private val coroutineChannel: Channel<Deferred<MessageBatch>> = Channel(Channel.UNLIMITED)
    private val messageHandler: MessageHandler<RawMessageBatch, MessageBatch>
    private val subscriber: RabbitMqSubscriber
    private val messageSender: MessageSender<MessageBatch>
    private val rabbitMQConnection: Connection

    init {
        messageHandler = object : MessageHandler<RawMessageBatch, MessageBatch>(
            DecodeProcessor(applicationContext.codec, applicationContext.messageToProtoConverter),
            coroutineContext,
            coroutineChannel
        ) {
            override fun toProtoMessage(byteArray: ByteArray): RawMessageBatch  = RawMessageBatch.parseFrom(byteArray)
        }
        subscriber = RabbitMqSubscriber(
                configuration.decoding.inParams.exchangeName,
                messageHandler,
                null, // FIXME handle cancellation
                configuration.decoding.inParams.queueName
        )
        rabbitMQConnection = applicationContext.connectionFactory.newConnection()
        val channel = rabbitMQConnection.createChannel()
        channel.exchangeDeclare(configuration.decoding.inParams.exchangeName, "direct")
        messageSender = DecodeMessageSender(
            newSingleThreadContext("sender-context"),
            coroutineChannel,
            configuration.decoding.outParams.filters.map {
                FilterChannelSender(
                    rabbitMQConnection.createChannel(),
                    DefaultFilterFactory().create(it),
                    it.exchangeName,
                    it.queueName
                )
            }
        )
    }

    fun start(rabbitMQParameters: RabbitMQParameters) {
        try {
            subscriber.startListening(
                rabbitMQParameters.host,
                rabbitMQParameters.vHost,
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

    fun close() {
        try {
            subscriber.close()
        } catch (exception: IOException) {
            logger.error(exception) { "could not close subscriber: $exception" }
        }
        try {
            messageSender.close()
        } catch (exception: IOException) {
            logger.error(exception) { "could not close message sender: $exception" }
        }
    }
}