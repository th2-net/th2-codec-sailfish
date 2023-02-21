/*
 *  Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.isValid
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.logId
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import mu.KotlinLogging
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

class EventBatchCollector(
    private val eventBatchRouter: MessageRouter<EventBatch>,
    maxBatchSizeInBytes: Long,
    maxBatchSizeInItems: Int,
    maxFlushTime: Long,
    numOfEventBatchCollectorWorkers: Int,
    private val boxBookName: String
) : AutoCloseable {
    private val scheduler = Executors.newScheduledThreadPool(numOfEventBatchCollectorWorkers)
    private val eventBatcher = EventBatcher(
        maxBatchSizeInBytes,
        maxBatchSizeInItems,
        maxFlushTime,
        scheduler,
        eventBatchRouter::sendAll
    )

    private lateinit var rootEventID: EventID
    private lateinit var decodeErrorGroupEventID: EventID
    private lateinit var encodeErrorGroupEventID: EventID

    private fun putEvent(event: Event) = eventBatcher.onEvent(event)

    fun createAndStoreDecodeErrorEvent(errorText: String, rawMessage: RawMessage, exception: Exception? = null) {
        try {
            val parentEventId =
                if (rawMessage.hasParentEventId()) rawMessage.parentEventId else getDecodeErrorGroupEventID()
            val event = createErrorEvent(
                "Cannot decode message for ${rawMessage.metadata.id.connectionId.toJson()}", errorText, exception, parentEventId,
                listOf<MessageID>(rawMessage.metadata.id)
            )
            logger.error { "${errorText}. Error event id: ${event.id.toJson()}" }
            putEvent(event)
        } catch (e: Exception) {
            logger.error(e) { "could not send codec error event. text: $errorText, message id: ${rawMessage.metadata.id.logId}, cause: $exception" }
        }
    }

    fun createAndStoreErrorEvent(
        errorText: String,
        exception: Exception,
        direction: AbstractSyncCodec.Direction,
        group: MessageGroup
    ) {
        try {
            val parentEventId = getParentEventIdFromGroup(direction, group)
            val messageIDs = getMessageIDsFromGroup(group)
            val event = createErrorEvent("Cannot process message group", errorText, exception, parentEventId, messageIDs)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toJson()}" }
            putEvent(event)
        } catch (e: Exception) {
            logger.error(e) { "could not send codec error event. text: $errorText, group id: ${ if (group.messagesList.isEmpty()) "" else group.getMessages(0).logId }, direction: $direction, cause: $exception" }
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
            logger.error(exception) { "could not send codec error event. name: $name, text: $errorText, cause: $exception" }
        }
    }

    fun initEventStructure(codecName: String, protocol: String, codecParameters: Map<String, String>?) {
        try {
            val event = com.exactpro.th2.common.event.Event
                .start()
                .name("Codec_${codecName}_${LocalDateTime.now()}")
                .type("CodecRoot")
                .apply {
                    bodyData(EventUtils.createMessageBean("Protocol: $protocol"))
                    if (codecParameters.isNullOrEmpty()) {
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
                .toProto(boxBookName)

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
            logger.error(exception) { "could not init root event structure" }
            throw exception
        }
    }

    private fun getDecodeErrorGroupEventID(): EventID {
        check(::decodeErrorGroupEventID.isInitialized) { "Decode root is not initialized" }
        return decodeErrorGroupEventID
    }

    private fun initDecodeEventRoot(parentEventId: EventID) {
        val event = com.exactpro.th2.common.event.Event
            .start()
            .name("DecodeError")
            .type("CodecErrorGroup")
            .toProto(parentEventId)
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

    private fun initEncodeEventRoot(parentEventId: EventID) {
        val event = com.exactpro.th2.common.event.Event
            .start()
            .name("EncodeError")
            .type("CodecErrorGroup")
            .toProto(parentEventId)
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
        parentEventId: EventID,
        messageIds: List<MessageID> = mutableListOf()
    ): Event = com.exactpro.th2.common.event.Event
        .start()
        .name(name)
        .type("CodecError")
        .status(com.exactpro.th2.common.event.Event.Status.FAILED)
        .bodyData(EventUtils.createMessageBean(errorText))
        .apply {
            if (exception != null) {
                exception(exception, true)
            }
            messageIds.forEach {
                if (it.isValid) {
                    messageID(it)
                }
            }
        }
        .toProto(parentEventId)


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

        eventBatcher.close()
        scheduler.shutdown()
        if (scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            logger.warn("Cannot shutdown scheduler for 5 seconds")
            scheduler.shutdownNow()
        }
        logger.info { "EventBatchCollector is closed. " }
    }

    @Suppress("unused")
    @JsonPropertyOrder("name", "value")
    private class ParametersRow(val name: String, val value: String) : IRow
}