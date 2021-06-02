/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.GeneratedMessageV3
import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec<T : GeneratedMessageV3, R : GeneratedMessageV3>(
    protected val sourceRouter: MessageRouter<T>,
    protected val targetRouter: MessageRouter<R>,
    protected val processor: AbstractCodecProcessor<T, R>,
    protected val eventBatchCollector: EventBatchCollector
) : AutoCloseable, MessageListener<T> {

    protected val logger = KotlinLogging.logger {}
    protected var tagretAttributes: String = ""


    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.tagretAttributes = targetAttributes
            sourceRouter.subscribeAll(this, sourceAttributes)
        } catch (exception: Exception) {
            when (exception) {
                is IOException,
                is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {
        val exceptions = mutableListOf<Exception>()

        sourceRouter.close()
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

    override fun handler(consumerTag: String?, message: T) {
        var protoResult: R? = null
        try {

            protoResult = processor.process(message)

            if (checkResult(protoResult))
                targetRouter.sendAll(protoResult, this.tagretAttributes)


        } catch (exception: RuntimeException) {
            eventBatchCollector.createAndStoreErrorEvent(
                "Cannot process message",
                exception,
                getDirection(),
                message
            )
        }
    }

    enum class Direction {
        ENCODE, DECODE
    }

    protected abstract fun getDirection(): Direction
    abstract fun parseProtoSourceFrom(data: ByteArray): T
    abstract fun checkResult(protoResult: R): Boolean
}