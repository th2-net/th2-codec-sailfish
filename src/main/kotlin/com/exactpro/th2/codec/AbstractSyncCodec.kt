/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.infra.grpc.Event
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.EventStatus
import com.exactpro.th2.schema.message.MessageListener
import com.exactpro.th2.schema.message.MessageRouter
import com.google.protobuf.ByteString.copyFrom
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.InvalidProtocolBufferException
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec<T: GeneratedMessageV3, R: GeneratedMessageV3>(
    protected val sourceRouter: MessageRouter<out T>,
    protected val targetRouter: MessageRouter<out R>,
    applicationContext: ApplicationContext,
    protected val processor: AbstractCodecProcessor<T, R>,
    protected val codecRootEvent: EventID?
): AutoCloseable, MessageListener<T> {

    protected val logger = KotlinLogging.logger {}
    protected val eventBatchRouter: MessageRouter<EventBatch>? = applicationContext.eventBatchRouter
    protected val context = applicationContext;

    protected var tagretAttributes : String = ""


    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.tagretAttributes = targetAttributes
            (sourceRouter as MessageRouter<T>).subscribeAll(this, sourceAttributes)
        } catch (exception: Exception) {
            when(exception) {
                is IOException,
                is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {
        val exceptions = mutableListOf<Exception>()

        sourceRouter.close()
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

    override fun handler(consumerTag: String?, message: T) {
        var protoResult: R? = null
        try {

            protoResult = processor.process(message)

            if (checkResult(protoResult))
                (targetRouter as MessageRouter<R>).sendAll(protoResult, this.tagretAttributes)


        } catch (exception: CodecException) {
            val parentEventId = getParentEventId(codecRootEvent, message, protoResult)
            if (parentEventId != null) {
                createAndStoreErrorEvent(exception, parentEventId)
            }
            logger.error(exception) {}
        }
    }

    override fun onClose() {
        super.onClose()
    }

    private fun createAndStoreErrorEvent(exception: CodecException, parentEventID: EventID) {
        if (eventBatchRouter != null) {
            try {
                eventBatchRouter.send(EventBatch.newBuilder().addEvents(
                    Event.newBuilder()
                        .setName("Codec error")
                        .setType("CodecError")
                        .setStatus(EventStatus.FAILED)
                        .setParentId(parentEventID)
                        .setBody(copyFrom("{\"message\":\"${exception.getAllMessages()}\"}", UTF_8))
                        .build()
                ).build(),
                    "publish", "event"
                )
            } catch (exception: Exception) {
                logger.warn(exception) { "could not send codec error event" }
            }
        }
    }

    private fun toProtoSource(message: Delivery): T {
        try {
            val protoMessage = parseProtoSourceFrom(message.body)
            logger.debug {
                val debugMessage = protoMessage.toDebugString()
                "received ${protoMessage::class.java.simpleName} from " +
                        "'${message.envelope.exchange}':'${message.envelope.routingKey}': $debugMessage"
            }
            return protoMessage
        } catch (exception: InvalidProtocolBufferException) {
            throw CodecException("'${message.envelope.exchange}':'${message.envelope.routingKey}': " +
                    "could not parse message body to proto", exception)
        }
    }

    abstract fun getParentEventId(codecRootID: EventID?, protoSource: T?, protoResult: R?): EventID?
    abstract fun parseProtoSourceFrom(data: ByteArray): T
    abstract fun checkResult(protoResult: R): Boolean
}