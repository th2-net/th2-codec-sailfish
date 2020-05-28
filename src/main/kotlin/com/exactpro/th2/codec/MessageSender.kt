package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.FilterChannelSender
import com.google.protobuf.GeneratedMessageV3
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import java.io.IOException
import kotlin.coroutines.CoroutineContext

abstract class MessageSender<R : GeneratedMessageV3>(
    private val context: CoroutineContext,
    private val codecChannel: Channel<Deferred<R>>,
    private val consumers: List<FilterChannelSender>
): AutoCloseable {
    private val logger = KotlinLogging.logger {  }
    private lateinit var job: Job

    fun start() {
        runBlocking(context) {
            job = launch {
                while (true) {
                    val codecResult = try {
                        codecChannel.receive().await()
                    } catch (exception: CodecException) {
                        logger.error(exception) { "could not get codec result" }
                        continue
                    }
                    val resultBatch = toCommonBatch(codecResult)
                    if (!resultBatch.isEmpty()) {
                        for (consumer in consumers) {
                            consumer.filterAndSend(resultBatch)
                        }
                    }
                }
            }
        }
    }

    abstract fun toCommonBatch(codecResult: R): CommonBatch

    override fun close() {
        try {
            job.cancel()
        } catch (exception: IOException) {
            logger.error(exception) { "could not close rabbitMQ connection" }
            throw RuntimeException(exception)
        }
    }
}