/*
 *  Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.GeneratedMessageV3
import mu.KotlinLogging
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import com.exactpro.th2.common.event.bean.Message as MessageEvent

private val logger = KotlinLogging.logger {}

class CollectorTask {
    @Volatile
    lateinit var eventBatchBuilder: EventBatch.Builder

    @Volatile
    lateinit var scheduledFuture: ScheduledFuture<*>

    @Volatile
    var isSent: Boolean = false

    fun update(
        event: Event,
        scheduleCollectorTask: (collectorTask: CollectorTask) -> ScheduledFuture<*>
    ) {
        eventBatchBuilder = EventBatch.newBuilder().setParentEventId(event.parentId)
        scheduledFuture = scheduleCollectorTask(this)
        isSent = false
    }
}

class EventBatchCollector(
    private val eventBatchRouter: MessageRouter<EventBatch>,
    private val maxBatchSize: Int,
    private val timeout: Long,
    private val numOfEventBatchCollectorWorkers: Int
) : AutoCloseable {
    private val collectorTasks = ConcurrentHashMap<EventID, CollectorTask>()
    private val scheduler = Executors.newScheduledThreadPool(numOfEventBatchCollectorWorkers)

    private lateinit var rootEventID: EventID
    private lateinit var decodeErrorGroupEventID: EventID
    private lateinit var encodeErrorGroupEventID: EventID

    private fun putEvent(event: Event) {
        val collectorTask = collectorTasks.computeIfAbsent(event.parentId) {
            CollectorTask().apply {
                update(event, this@EventBatchCollector::scheduleCollectorTask)
            }
        }

        synchronized(collectorTask) {
            if (collectorTask.isSent) {
                collectorTasks[event.parentId] = collectorTask.apply {
                    update(event, this@EventBatchCollector::scheduleCollectorTask)
                }
            }
            collectorTask.eventBatchBuilder.addEvents(event)
            if (collectorTask.eventBatchBuilder.eventsList.size == maxBatchSize) {
                collectorTask.isSent = true
                collectorTask.scheduledFuture.cancel(true)
                eventBatchRouter.send(collectorTask.eventBatchBuilder.build())
                collectorTasks.remove(event.parentId)
            }
        }
    }

    private fun scheduleCollectorTask(collectorTask: CollectorTask): ScheduledFuture<*> =
        scheduler.schedule(
            { executeCollectorTask(collectorTask) },
            timeout, TimeUnit.SECONDS
        )

    private fun executeCollectorTask(collectorTask: CollectorTask) {
        synchronized(collectorTask) {
            if (!collectorTask.isSent) {
                collectorTask.isSent = true
                eventBatchRouter.send(collectorTask.eventBatchBuilder.build())
                collectorTasks.remove(collectorTask.eventBatchBuilder.parentEventId)
            }
        }
    }

    fun createAndStoreDecodeErrorEvent(errorText: String, rawMessage: RawMessage) {
        try {
            val parentEventID = if (rawMessage.hasParentEventId()) rawMessage.parentEventId else getDecodeErrorGroupEventID()
            val event = createErrorEvent(errorText, null, parentEventID, listOf<MessageID>(rawMessage.metadata.id))
            logger.error { "${errorText}. Error event id: ${event.id.toDebugString()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun <T : GeneratedMessageV3> createAndStoreErrorEvent(
        errorText: String,
        exception: RuntimeException,
        direction: AbstractSyncCodec.Direction,
        message: T
    ) {
        try {
            val parentEventID = getParentEventId(direction, message)
            val messageIDs = getMessageIDs(message)
            val event = createErrorEvent(errorText, exception, parentEventID, messageIDs)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toDebugString()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun createAndStoreErrorEvent(
        errorText: String,
        exception: RuntimeException
    ) {
        try {
            val event = createErrorEvent(errorText, exception, rootEventID)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toDebugString()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun createAndStoreRootEvent(codecName: String) {
        try {
            val event = com.exactpro.th2.common.event.Event.start()
                .name("Codec_${codecName}_${LocalDateTime.now()}")
                .type("CodecRoot")
                .toProto(null)

            rootEventID = event.id
            logger.info { "root event id: ${event.id.toDebugString()}" }
            eventBatchRouter.send(
                EventBatch.newBuilder()
                    .addEvents(event)
                    .build()
            )

        } catch (exception: Exception) {
            logger.warn(exception) { "could not store root event" }
        }
    }


    private fun getDecodeErrorGroupEventID(): EventID {
        try {
            if (!::decodeErrorGroupEventID.isInitialized) {
                synchronized(decodeErrorGroupEventID) {
                    if (!::decodeErrorGroupEventID.isInitialized) {
                        val event = com.exactpro.th2.common.event.Event.start()
                            .name("DecodeError")
                            .type("CodecErrorGroup")
                            .toProto(rootEventID)
                        decodeErrorGroupEventID = event.id

                        logger.info { "DecodeError group event id: ${event.id.toDebugString()}" }
                        eventBatchRouter.send(
                            EventBatch.newBuilder()
                                .addEvents(event)
                                .build()
                        )
                    }
                }
            }
        } catch (exception: Exception) {
            logger.warn(exception) { "could not store DecodeError group event" }
        }
        return decodeErrorGroupEventID
    }

    private fun getEncodeErrorGroupEventID(): EventID {
        try {
            if (!::encodeErrorGroupEventID.isInitialized) {
                synchronized(encodeErrorGroupEventID) {
                    if (!::encodeErrorGroupEventID.isInitialized) {
                        val event = com.exactpro.th2.common.event.Event.start()
                            .name("EncodeError")
                            .type("CodecErrorGroup")
                            .toProto(rootEventID)
                        encodeErrorGroupEventID = event.id

                        logger.info { "EncodeError group event id: ${event.id.toDebugString()}" }
                        eventBatchRouter.send(
                            EventBatch.newBuilder()
                                .addEvents(event)
                                .build()
                        )
                    }
                }
            }
        } catch (exception: Exception) {
            logger.warn(exception) { "could not store EncodeError group event" }
        }
        return encodeErrorGroupEventID
    }

    private fun createErrorEvent(
        errorText: String,
        exception: Exception?,
        parentEventID: EventID,
        messageIDS: List<MessageID> = mutableListOf()
    ): Event = com.exactpro.th2.common.event.Event.start()
        .name(exception?.message ?: errorText)
        .type("CodecError")
        .status(com.exactpro.th2.common.event.Event.Status.FAILED)
        .bodyData(MessageEvent().apply {
            data = errorText
            type = "message"
        }).apply {
            if (exception != null) {
                exception(exception, true)
            }
            messageIDS.forEach {
                messageID(it)
            }
        }
        .toProto(parentEventID)


    private fun <T : GeneratedMessageV3> getMessageIDs(message: T) = mutableListOf<MessageID>().apply {
        if (message is Message) {
            message.metadata.id
        } else if (message is RawMessage) {
            message.metadata.id
        }
    }

    private fun <T : GeneratedMessageV3> getParentEventId(direction: AbstractSyncCodec.Direction, message: T): EventID {
        if (message is Message) {
            if (message.hasParentEventId()) {
                return message.parentEventId
            }
        } else if (message is RawMessage) {
            if (message.hasParentEventId()) {
                return message.parentEventId
            }
        }
        return when (direction) {
            AbstractSyncCodec.Direction.ENCODE -> getEncodeErrorGroupEventID()
            AbstractSyncCodec.Direction.DECODE -> getDecodeErrorGroupEventID()
        }
    }

    override fun close() {
        logger.info { "Closing EventBatchCollector. Sending unsent batches." }

        collectorTasks.values.forEach {
            synchronized(it) {
                if (!it.isSent) {
                    it.isSent = true
                    it.scheduledFuture.cancel(true)
                    eventBatchRouter.send(it.eventBatchBuilder.build())
                }
            }
        }
        collectorTasks.clear()
        scheduler.shutdown()
        scheduler.awaitTermination(5, TimeUnit.SECONDS)

        logger.info { "EventBatchCollector is closed. " }
    }

}