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
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec(
    private val router: MessageRouter<MessageGroupBatch>,
    private val applicationContext: ApplicationContext
) : AutoCloseable, MessageListener<MessageGroupBatch> {

    private var targetAttributes: String = ""

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

    override fun handle(deliveryMetadata: DeliveryMetadata, groupBatch: MessageGroupBatch) {
        if (groupBatch.groupsCount < 1) {
            return
        }

        val resultBuilder = MessageGroupBatch.newBuilder()
        groupBatch.groupsList.filter { it.messagesCount > 0 }.forEachIndexed { index, group ->
            try {
                processMessageGroup(group).apply {
                    if (this != null && checkResult(this)) {
                        resultBuilder.addGroups(this)
                    }
                }
            } catch (exception: Exception) {
                applicationContext.eventBatchCollector.createAndStoreErrorEvent(
                    "Cannot process not empty group number ${index + 1}",
                    exception,
                    getDirection(),
                    group
                )
            }
        }

        val result = resultBuilder.build()
        if (checkResultBatch(result)) {
            router.sendAll(result, this.targetAttributes)
        }
    }

    enum class Direction {
        ENCODE, DECODE
    }

    protected abstract fun getDirection(): Direction

    protected abstract fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean

    protected abstract fun processMessageGroup(it: MessageGroup): MessageGroup?

    abstract fun checkResult(protoResult: MessageGroup): Boolean
}