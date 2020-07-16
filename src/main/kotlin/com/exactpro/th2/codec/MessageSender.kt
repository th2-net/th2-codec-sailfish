/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.FilterChannelSender
import com.google.protobuf.GeneratedMessageV3
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.io.IOException
import kotlin.coroutines.CoroutineContext

abstract class MessageSender<R : GeneratedMessageV3>(
    private val context: CoroutineContext,
    private val codecChannel: Channel<Deferred<R>>,
    private val consumers: List<FilterChannelSender>
) : AutoCloseable {
    private val logger = KotlinLogging.logger { }
    private lateinit var job: Job

    fun start() {
        job = CoroutineScope(context).launch {
            while (true) {
                val codecResult = try {
                    codecChannel.receive().await()
                } catch (exception: CodecException) {
                    logger.error(exception) { "exception upon message handling" }
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