package com.exactpro.th2.codec


import com.exactpro.th2.codec.util.getAllMessages
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timerTask
import kotlin.concurrent.withLock

private val logger = KotlinLogging.logger {}

class CollectorTask(val eventBatchBuilder: EventBatch.Builder, val timerTask: TimerTask)

class EventBatchCollector(
    private val eventBatchRouter: MessageRouter<EventBatch>,
    private val maxBatchSize: Int,
    private val timeout: Long
) : AutoCloseable {
    private val collectorTasks = mutableMapOf<EventID, CollectorTask>()
    private val lock = ReentrantLock()

    private lateinit var rootEventID: EventID
    private lateinit var decodeErrorGroupEventID: EventID
    private lateinit var encodeErrorGroupEventID: EventID

    private fun putEvent(event: Event) {
        lock.withLock {
            val batchAndTask = collectorTasks.getOrPut(event.parentId) {
                val builder = EventBatch.newBuilder().setParentEventId(event.parentId)
                val task = timerTask { sendEventBatch(event.parentId) }
                Timer().schedule(task, timeout)
                CollectorTask(builder, task)
            }

            batchAndTask.eventBatchBuilder.addEvents(event)
            if (batchAndTask.eventBatchBuilder.eventsList.size == maxBatchSize) {
                batchAndTask.timerTask.cancel()
                sendEventBatch(batchAndTask.eventBatchBuilder.build())
                collectorTasks.remove(event.parentId)
            }
        }
    }

    private fun sendEventBatch(eventID: EventID) {
        lock.withLock {
            collectorTasks[eventID]?.eventBatchBuilder?.build()?.let { sendEventBatch(it) }
            collectorTasks.remove(eventID)
        }
    }

    private fun sendEventBatch(eventBatch: EventBatch) {
        eventBatchRouter.send(eventBatch, "publish", "event")
    }

    fun createAndStoreRootEvent(codecName: String) {
        try {
            val event = com.exactpro.th2.common.event.Event.start()
                .name("Codec_${codecName}_${LocalDateTime.now()}")
                .type("CodecRoot")
                .toProtoEvent(null)

            rootEventID = event.id
            logger.info { "root event id: ${event.id.toDebugString()}" }
            sendEventBatch(
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
            if (!decodeErrorGroupEventID.isInitialized) {
                val event = com.exactpro.th2.common.event.Event.start()
                    .name("DecodeError")
                    .type("CodecErrorGroup")
                    .toProtoEvent(rootEventID.id)
                decodeErrorGroupEventID = event.id

                logger.info { "DecodeError group event id: ${event.id.toDebugString()}" }
                sendEventBatch(
                    EventBatch.newBuilder()
                        .addEvents(event)
                        .build()
                )
            }
        } catch (exception: Exception) {
            logger.warn(exception) { "could not store DecodeError group event" }
        }
        return decodeErrorGroupEventID
    }

    private fun getEncodeErrorGroupEventID(): EventID {
        try {
            if (!encodeErrorGroupEventID.isInitialized) {
                val event = com.exactpro.th2.common.event.Event.start()
                    .name("EncodeError")
                    .type("CodecErrorGroup")
                    .toProtoEvent(rootEventID.id)
                encodeErrorGroupEventID = event.id

                logger.info { "EncodeError group event id: ${event.id.toDebugString()}" }
                sendEventBatch(
                    EventBatch.newBuilder()
                        .addEvents(event)
                        .build()
                )
            }
        } catch (exception: Exception) {
            logger.warn(exception) { "could not store EncodeError group event" }
        }
        return encodeErrorGroupEventID
    }

    fun createAndStoreDecodeErrorEvent(errorText: String, rawMessage: RawMessage) {
        try {
            val parentEventID =
                if (rawMessage.hasParentEventId()) rawMessage.parentEventId else getDecodeErrorGroupEventID()
            val event = createErrorEvent(errorText, null, parentEventID, listOf<MessageID>(rawMessage.metadata.id))
            logger.error { "${errorText}. Error event id: ${event.id.toDebugString()}" }
            storeErrorEvent(parentEventID, event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    fun createAndStoreErrorEvent(
        errorText: String,
        exception: RuntimeException,
        direction: AbstractSyncCodec.Direction,
        group: MessageGroup
    ) {
        try {
            val parentEventID = getParentEventIdFromGroup(direction, group)
            val messageIDs = getMessageIDsFromGroup(group)
            val event = createErrorEvent(errorText, exception, parentEventID, messageIDs)
            logger.error(exception) { "${errorText}. Error event id: ${event.id.toDebugString()}" }
            storeErrorEvent(parentEventID, event)
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
            storeErrorEvent(rootEventID, event)
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    private fun createErrorEvent(
        errorText: String,
        exception: Exception?,
        parentEventID: EventID,
        messageIDS: List<MessageID> = mutableListOf()
    ): Event {
        val eventName = exception?.message ?: errorText
        var event = com.exactpro.th2.common.event.Event.start()
            .name(eventName)
            .type("CodecError")
            .status(com.exactpro.th2.common.event.Event.Status.FAILED)

        event = event.bodyData(Message().apply {
            data = errorText
            type = "message"
        })
        exception?.getAllMessages()?.forEach {
            event = event.bodyData(Message().apply {
                data = it
                type = "message"
            })
        }
        messageIDS.forEach {
            event = event.messageID(it)
        }
        return event.toProtoEvent(parentEventID.id)
    }

    private fun storeErrorEvent(parentEventID: EventID, event: Event) {
        if (parentEventID.id == rootEventID.id) {
            putEvent(event)
        } else {
            sendEventBatch(
                EventBatch.newBuilder()
                    .addEvents(event)
                    .build()
            )
        }
    }

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
        lock.withLock {
            collectorTasks.values.forEach {
                it.timerTask.cancel()
                sendEventBatch(it.eventBatchBuilder.build())
            }
            collectorTasks.clear()
        }
        logger.info { "EventBatchCollector is closed. " }
    }
}