/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec.transport

import com.exactpro.th2.codec.AbstractCodec
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture


abstract class AbstractTransportCodec(
    router: MessageRouter<GroupBatch>,
    applicationContext: ApplicationContext,
    sourceAttributes: String,
    targetAttributes: String,
) : AbstractCodec<GroupBatch>(
    applicationContext,
    router,
    sourceAttributes,
    targetAttributes
), MessageListener<GroupBatch> {

    private val async = applicationContext.enabledVerticalScaling && Runtime.getRuntime().availableProcessors() > 1
    override fun process(batch: GroupBatch): GroupBatch? {
        if (batch.groups.isEmpty()) {
            return null
        }

        val resultBuilder = GroupBatch.builder()

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<MessageGroup?>>(batch.groups.size) { index ->
                CompletableFuture.supplyAsync { runProcessMessageGroup(batch, index) }
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultBuilder::addGroup) }
            }.get()
        } else {
            batch.groups.indices.forEach { index ->
                runProcessMessageGroup(batch, index)?.run(resultBuilder::addGroup)
            }
        }
        resultBuilder.apply {
            setSessionGroup(batch.sessionGroup)
            setBook(batch.book)
        }
        val result = resultBuilder.build()
        return if (checkResultBatch(result)) result else null
    }

    override val GroupBatch.externalQueue: String
        get(): String {
            K_LOGGER.debug { "Sending via external queue isn't supported" }
            return "" // TODO: implement
        }

    protected fun checkProtocol(message: Message<*>, expectedProtocol: String) = message.protocol.let {
        it.isBlank() || expectedProtocol.equals(it, ignoreCase = true)
    }

    protected abstract fun getDirection(): Direction
    protected abstract fun checkResultBatch(resultBatch: GroupBatch): Boolean

    protected abstract fun processMessageGroup(batch: GroupBatch, group: MessageGroup): MessageGroup?

    protected abstract fun checkResult(protoResult: MessageGroup): Boolean

    private fun runProcessMessageGroup(
        batch: GroupBatch,
        index: Int
    ): MessageGroup? {
        val messageGroup = batch.groups[index]
        if (messageGroup.messages.isEmpty()) return null

        try {
            return processMessageGroup(batch, messageGroup)?.takeIf(::checkResult)
        } catch (exception: Exception) {
            eventBatchCollector.createAndStoreErrorEvent(
                "Cannot process not empty group number ${index + 1}",
                exception,
                getDirection(),
                batch.book,
                batch.sessionGroup,
                messageGroup
            )
        }

        return null
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}