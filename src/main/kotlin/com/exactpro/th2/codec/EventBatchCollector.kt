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

import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.MessageRouter
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import mu.KotlinLogging
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

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
    numOfEventBatchCollectorWorkers: Int
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

    fun createAndStoreDecodeErrorEvent(errorText: String, rawMessage: RawMessage, exception: Exception? = null) {
        try {
            val parentEventID =
                if (rawMessage.hasParentEventId()) rawMessage.parentEventId else getDecodeErrorGroupEventID()
            val event = createErrorEvent(
                "Cannot decode message for ${rawMessage.metadata.id.connectionId.toJson()}", errorText, exception, parentEventID,
                listOf<MessageID>(rawMessage.metadata.id)
            )
            logger.error { "${errorText}. Error event id: ${event.id.toJson()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun createAndStoreErrorEvent(
        errorText: String,
        exception: Exception,
        direction: AbstractSyncCodec.Direction,
        group: MessageGroup
    ) {
        try {
            val parentEventID = getParentEventIdFromGroup(direction, group)
            val messageIDs = getMessageIDsFromGroup(group)
            val event = createErrorEvent("Cannot process message group", errorText, exception, parentEventID, messageIDs)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toJson()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun createAndStoreErrorEvent(
        name: String,
        errorText: String,
        exception: RuntimeException
    ) {
        try {
            val event = createErrorEvent(name, errorText, exception, rootEventID)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toJson()}" }
            putEvent(event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun initEventStructure(codecName: String, protocol: String, codecParameters: Map<String, String>?) {
        try {
            val event = com.exactpro.th2.common.event.Event.start()
                .name("Codec_${codecName}_${LocalDateTime.now()}")
                .type("CodecRoot")
                .apply {
                    bodyData(EventUtils.createMessageBean("Protocol: $protocol"))
                    if (codecParameters == null || codecParameters.isEmpty()) {
                        bodyData(EventUtils.createMessageBean("No parameters specified for codec"))
                    } else {
                        bodyData(EventUtils.createMessageBean("Codec parameters:"))
                        bodyData(TableBuilder<ParametersRow>()
                            .apply {
                                codecParameters.forEach { (name, value) ->
                                    row(ParametersRow(name, value))
                                }
                            }
                            .build())
                    }
                }
                .toProto(null)

            rootEventID = event.id
            logger.info { "root event id: ${event.id.toJson()}" }
            eventBatchRouter.send(
                EventBatch.newBuilder()
                    .addEvents(event)
                    .build()
            )
            initDecodeEventRoot(rootEventID)
            initEncodeEventRoot(rootEventID)

        } catch (exception: Exception) {
            logger.warn(exception) { "could not init root event structure" }
            throw exception
        }
    }

    private fun getDecodeErrorGroupEventID(): EventID {
        check(::decodeErrorGroupEventID.isInitialized) { "Decode root is not initialized" }
        return decodeErrorGroupEventID
    }

    private fun initDecodeEventRoot(parent: EventID) {
        val event = com.exactpro.th2.common.event.Event.start()
            .name("DecodeError")
            .type("CodecErrorGroup")
            .toProto(parent)
        decodeErrorGroupEventID = event.id

        logger.info { "DecodeError group event id: ${event.id.toJson()}" }
        eventBatchRouter.send(
            EventBatch.newBuilder()
                .addEvents(event)
                .build()
        )
    }

    private fun getEncodeErrorGroupEventID(): EventID {
        check(::encodeErrorGroupEventID.isInitialized) { "Encode root is not initialized" }
        return encodeErrorGroupEventID
    }

    private fun initEncodeEventRoot(parent: EventID) {
        val event = com.exactpro.th2.common.event.Event.start()
            .name("EncodeError")
            .type("CodecErrorGroup")
            .toProto(parent)
        encodeErrorGroupEventID = event.id

        logger.info { "EncodeError group event id: ${event.id.toJson()}" }
        eventBatchRouter.send(
            EventBatch.newBuilder()
                .addEvents(event)
                .build()
        )
    }

    private fun createErrorEvent(
        name: String,
        errorText: String,
        exception: Exception?,
        parentEventID: EventID,
        messageIDS: List<MessageID> = mutableListOf()
    ): Event = com.exactpro.th2.common.event.Event.start()
        .name(name)
        .type("CodecError")
        .status(com.exactpro.th2.common.event.Event.Status.FAILED)
        .bodyData(EventUtils.createMessageBean(errorText))
        .apply {
            if (exception != null) {
                exception(exception, true)
            }
            messageIDS.forEach {
                messageID(it)
            }
        }
        .toProto(parentEventID)


    private fun getMessageIDsFromGroup(group: MessageGroup) = mutableListOf<MessageID>().apply {
        group.messagesList.forEach {
            if (it.hasMessage()) {
                add(it.message.metadata.id)
            } else {
                add(it.rawMessage.metadata.id)
            }
        }
    }

    private fun getParentEventIdFromGroup(direction: AbstractSyncCodec.Direction, group: MessageGroup): EventID {
        if (group.messagesCount != 0) {
            val firstMessageInList = group.messagesList.first()
            if (firstMessageInList.hasMessage()) {
                if (firstMessageInList.message.hasParentEventId()) {
                    return firstMessageInList.message.parentEventId
                }
            } else {
                if (firstMessageInList.rawMessage.hasParentEventId()) {
                    return firstMessageInList.rawMessage.parentEventId
                }
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
        if (scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            logger.warn("Cannot shutdown scheduler for 5 seconds")
            scheduler.shutdownNow()
        }
        logger.info { "EventBatchCollector is closed. " }
    }

    @JsonPropertyOrder("name", "value")
    private class ParametersRow(val name: String, val value: String) : IRow
}