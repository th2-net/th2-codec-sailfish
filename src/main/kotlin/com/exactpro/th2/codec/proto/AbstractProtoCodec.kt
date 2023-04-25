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
package com.exactpro.th2.codec.proto

import com.exactpro.th2.codec.AbstractCodec
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import java.util.concurrent.CompletableFuture


abstract class AbstractProtoCodec(
    router: MessageRouter<MessageGroupBatch>,
    applicationContext: ApplicationContext,
    sourceAttributes: String,
    targetAttributes: String,
) : AbstractCodec<MessageGroupBatch>(
    applicationContext,
    router,
    sourceAttributes,
    targetAttributes
), MessageListener<MessageGroupBatch> {

    private val async = applicationContext.enabledVerticalScaling && Runtime.getRuntime().availableProcessors() > 1
    override fun process(batch: MessageGroupBatch): MessageGroupBatch? {
        if (batch.groupsCount < 1) {
            return null
        }

        val resultBuilder = MessageGroupBatch.newBuilder()

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<MessageGroup?>> (batch.groupsCount) { index ->
                CompletableFuture.supplyAsync { runProcessMessageGroup(batch, index) }
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultBuilder::addGroups) }
            }.get()
        } else {
            batch.groupsList.indices.forEach { index ->
                runProcessMessageGroup(batch, index)?.run(resultBuilder::addGroups)
            }
        }
        val result = resultBuilder.build()
        return if (checkResultBatch(result)) result else null
    }

    override val MessageGroupBatch.externalQueue: String
        get() = metadata.externalQueue
    protected abstract fun getDirection(): Direction
    protected abstract fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean

    protected abstract fun processMessageGroup(batch: MessageGroupBatch, messageGroup: MessageGroup): MessageGroup?

    protected abstract fun checkResult(protoResult: MessageGroup): Boolean

    private fun runProcessMessageGroup(
        batch: MessageGroupBatch,
        index: Int
    ): MessageGroup? {
        val group = batch.getGroups(index)
        if (group.messagesCount == 0) return null

        try {
            return processMessageGroup(batch, group)?.takeIf(::checkResult)
        } catch (exception: Exception) {
            eventBatchCollector.createAndStoreErrorEvent(
                "Cannot process not empty group number ${index + 1}",
                exception,
                getDirection(),
                group
            )
        }

        return null
    }
}