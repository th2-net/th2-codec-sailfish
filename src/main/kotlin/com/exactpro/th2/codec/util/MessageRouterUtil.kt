package com.exactpro.th2.codec.util

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging

val logger = KotlinLogging.logger {}

fun MessageRouter<EventBatch>.sendError(parentEventID: EventID, errStr: String) {
    try {
        send(
            EventBatch.newBuilder().addEvents(
                Event.start()
                    .name("Codec error")
                    .type("CodecError")
                    .status(Event.Status.FAILED)
                    .bodyData(Message().apply {
                        data = errStr
                    })
                    .toProtoEvent(parentEventID.id)
            ).build(),
            "publish", "event"
        )
    } catch (exception: Exception) {
        logger.warn(exception) { "could not send codec error event" }
    }
}