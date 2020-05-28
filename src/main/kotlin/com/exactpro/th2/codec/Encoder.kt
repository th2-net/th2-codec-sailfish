package com.exactpro.th2.codec

import com.exactpro.th2.RabbitMqSubscriber
import com.exactpro.th2.codec.filter.DefaultFilterFactory
import com.exactpro.th2.codec.filter.FilterChannelSender
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.rabbitmq.client.Connection
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import java.io.IOException
import java.lang.Runtime.getRuntime

@ObsoleteCoroutinesApi
class Encoder(configuration: Configuration, applicationContext: ApplicationContext) {
    private val logger = KotlinLogging.logger {}
    private val coroutineContext = newFixedThreadPoolContext(getRuntime().availableProcessors(), "encoder-context")
    private val coroutineChannel: Channel<Deferred<RawMessageBatch>> = Channel(Channel.UNLIMITED)
    private val messageHandler: MessageHandler<MessageBatch, RawMessageBatch>
    private val subscriber: RabbitMqSubscriber
    private val messageSender: MessageSender<RawMessageBatch>
    private val rabbitMQConnection: Connection

    init {
        messageHandler = object : MessageHandler<MessageBatch, RawMessageBatch>(
            EncodeProcessor(applicationContext.codec, applicationContext.protoToIMessageConverter),
            coroutineContext,
            coroutineChannel
        ) {
            override fun toProtoMessage(byteArray: ByteArray): MessageBatch = MessageBatch.parseFrom(byteArray)
        }
        subscriber = RabbitMqSubscriber(
            configuration.encoding.inParams.exchangeName,
            messageHandler,
            null, // FIXME handle cancellation
            configuration.encoding.inParams.queueName
        )
        rabbitMQConnection = applicationContext.connectionFactory.newConnection()
        messageSender = EncodeMessageSender(
            newSingleThreadContext("encode-sender-context"),
            coroutineChannel,
            configuration.encoding.outParams.filters.map {
                FilterChannelSender(
                    rabbitMQConnection.createChannel(),
                    DefaultFilterFactory().create(it),
                    it.exchangeName,
                    it.queueName
                )
            }
        )
    }

    fun close() {
        try {
            subscriber.close()
        } catch (exception: IOException) {
            logger.error(exception) { "could not close subscriber: $exception" }
        }
    }
}