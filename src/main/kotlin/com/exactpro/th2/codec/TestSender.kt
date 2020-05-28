package com.exactpro.th2.codec

import com.exactpro.th2.infra.grpc.*
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.rabbitmq.client.ConnectionFactory

fun main() {
    val connectionFactory = ConnectionFactory()
    connectionFactory.host = "10.64.66.114"
    connectionFactory.virtualHost = "th2"
    connectionFactory.port = 5672
    connectionFactory.username =  "th2"
    connectionFactory.password =  "ahw3AeWa"
    val connection = connectionFactory.newConnection()
    val channel = connection.createChannel()

    val createBatch = createBatch()
    println("batch created")
    channel.basicPublish("codec_exchange", "codec_in", null, createBatch.toByteArray())
    println("batch sent")
    connection.close(3000)
}

fun createBatch(): RawMessageBatch {
    val rawMessageBatch = RawMessageBatch.newBuilder()
        .addMessages(createMessage(byteArrayOf(
                // TCP Soup BIN:
                0x00, 21, // Length
                // Length
                0x53 // PacketType = 'S' - SequencedMessage
            ))
        ).addMessages(createMessage(
                    byteArrayOf(
                        // Body
                        0x44, // MessageType = 'D' - OrderDeleted
                        // Message Payload
                        // MessageType = 'D' - OrderDeleted
                        // Message Payload
                        0x00, 0x00, 0x00, 0x03, // Timestamp = 3
                        // Timestamp = 3
                        0x00, 0x04, // TradeDate = 4
                        // TradeDate = 4
                        0x00, 0x00, 0x00, 0x05, // TradeableInstrumentId = 5
                        // TradeableInstrumentId = 5
                        0x53, // Side = 'S' = Sell
                        // Side = 'S' = Sell
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07 // OrderID = 7
                    )
                )

        ).build()
    return rawMessageBatch
}

fun createMessage(bytes: ByteArray):RawMessage {
    return RawMessage.newBuilder().setMetadata(
        RawMessageMetadata.newBuilder().setId(
            MessageID.newBuilder().setDirection(Direction.FIRST).setConnectionId(
                ConnectionID.newBuilder().setSessionAlias("test_session")
            )
        ).setTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
    ).setBody(ByteString.copyFrom(bytes)).build()
}
