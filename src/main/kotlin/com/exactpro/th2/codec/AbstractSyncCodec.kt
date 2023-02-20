/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException
import mu.KotlinLogging


abstract class AbstractSyncCodec(
    private val router: MessageRouter<MessageGroupBatch>,
    private val applicationContext: ApplicationContext
) : AutoCloseable, MessageListener<MessageGroupBatch> {

    protected val logger = KotlinLogging.logger {}
    private var targetAttributes: String = ""
    private val enabledExternalRouting: Boolean = applicationContext.enabledExternalRouting
    private val async = Runtime.getRuntime().availableProcessors() > 1



    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.targetAttributes = targetAttributes
            router.subscribeAll(this, sourceAttributes)
        } catch (exception: Exception) {
            when (exception) {
                is IOException,
                is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {
        router.close()
    }

    private fun close(closeable: AutoCloseable, name: String, exceptions: MutableList<Exception>) {
        try {
            closeable.close()
        } catch (exception: Exception) {
            exceptions.add(RuntimeException("could not close '$name'. Reason: ${exception.message}", exception))
        }
    }

    private fun processMessageGroupAsync(index: Int, group: MessageGroup) = CompletableFuture.supplyAsync { runProcessMessageGroup(index, group) }

    override fun handle(deliveryMetadata: DeliveryMetadata?, groupBatch: MessageGroupBatch) {
        if (groupBatch.groupsCount < 1) {
            return
        }

        val resultBuilder = MessageGroupBatch.newBuilder()

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<MessageGroup?>> (groupBatch.groupsCount) {
                processMessageGroupAsync(it, groupBatch.getGroups(it))
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultBuilder::addGroups) }
            }.get()
        } else {
            groupBatch.groupsList.forEachIndexed { index, group ->
                runProcessMessageGroup(index, group)?.run(resultBuilder::addGroups)
            }
        }
        val result = resultBuilder.build()
        if (checkResultBatch(result)) {
            val externalQueue = result.metadata.externalQueue
            if (enabledExternalRouting && externalQueue.isNotBlank() && isTransformationComplete(result)) {
                router.sendExclusive(externalQueue, result)
                return
            }
            router.sendAll(result, this.targetAttributes)
        }
    }

    private fun runProcessMessageGroup(
        index: Int,
        group: MessageGroup
    ): MessageGroup? {
        if (group.messagesCount == 0) return null

        try {
            return processMessageGroup(group)?.takeIf(::checkResult)
        } catch (exception: Exception) {
            applicationContext.eventBatchCollector.createAndStoreErrorEvent(
                "Cannot process not empty group number ${index + 1}",
                exception,
                getDirection(),
                group
            )
        }

        return null
    }

    enum class Direction {
        ENCODE, DECODE
    }
    protected abstract fun getDirection(): Direction

    protected abstract fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean

    protected abstract fun processMessageGroup(messageGroup: MessageGroup): MessageGroup?

    abstract fun checkResult(protoResult: MessageGroup): Boolean

    abstract fun isTransformationComplete(protoResult: MessageGroupBatch): Boolean
}