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
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec(
    protected val router: MessageRouter<MessageGroupBatch>,
    protected val applicationContext: ApplicationContext
) : AutoCloseable, MessageListener<MessageGroupBatch> {

    protected val logger = KotlinLogging.logger {}
    protected var tagretAttributes: String = ""


    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.tagretAttributes = targetAttributes
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
        val exceptions = mutableListOf<Exception>()

        router.close()
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

    private fun processMessageGroupAsync(group: MessageGroup, index: Int) = CompletableFuture.supplyAsync { runProcessMessageGroup(group, index) }

    private val async = Runtime.getRuntime().availableProcessors() > 1
    override fun handler(consumerTag: String?, groupBatch: MessageGroupBatch) {
        if (groupBatch.groupsCount < 1) {
            return
        }
        val resultBuilder = MessageGroupBatch.newBuilder()

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<MessageGroup?>> (groupBatch.groupsCount) {
                processMessageGroupAsync(groupBatch.getGroups(it), it)
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.apply {
                        if (it.get() != null && checkResult(it.get()!!)) {
                            resultBuilder.addGroups(it.get())
                        }
                    }
                }
            }.get()
        } else {

            groupBatch.groupsList.filter { it.messagesCount > 0 }.forEachIndexed { index, group ->
                runProcessMessageGroup(group, index).apply {
                    if (this != null && checkResult(this)) {
                        resultBuilder.addGroups(this)
                    }
                }
            }
        }
        val result = resultBuilder.build()
        if (checkResultBatch(result)) {
            router.sendAll(result, this.tagretAttributes)
        }
    }

    private fun runProcessMessageGroup(
        group: MessageGroup,
        index: Int
    ): MessageGroup? {
        try {
            return processMessageGroup(group)
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
//
    enum class Direction {
        ENCODE, DECODE
    }
    protected abstract fun getDirection(): Direction

    protected abstract fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean

    protected abstract fun processMessageGroup(messageGroup: MessageGroup): MessageGroup?

    abstract fun checkResult(protoResult: MessageGroup): Boolean
}