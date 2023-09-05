/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import java.io.IOException
import java.util.concurrent.TimeoutException

abstract class AbstractCodec<B>(
    applicationContext: ApplicationContext,
    private val router: MessageRouter<B>,
    private val sourceAttributes: String,
    private val targetAttributes: String,
) : AutoCloseable, MessageListener<B> {

    protected val eventBatchCollector = applicationContext.eventBatchCollector

    private val enabledExternalRouting: Boolean = applicationContext.enabledExternalRouting

    private val monitor = runCatching {
        router.subscribeAll(this, sourceAttributes)
    }.onFailure { exception ->
        when (exception) {
            is IOException,
            is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
            else -> throw DecodeException("could not start decoder", exception)
        }
    }.getOrThrow()

    override fun close() {
        monitor.unsubscribe()
    }

    override fun handle(deliveryMetadata: DeliveryMetadata, groupBatch: B) {
        process(groupBatch)?.let { result ->
            val externalQueue = result.externalQueue
            if (enabledExternalRouting && externalQueue.isNotBlank() && isTransformationComplete(result)) {
                router.sendExclusive(externalQueue, result)
                return
            }
            router.sendAll(result, this.targetAttributes)
        }
    }

    protected abstract val B.externalQueue: String
    protected abstract fun process(batch: B): B?
    protected abstract fun isTransformationComplete(result: B): Boolean

    enum class Direction {
        ENCODE, DECODE
    }
}