package com.exactpro.th2.codec.filter

import com.exactpro.th2.infra.grpc.MessageID
import com.google.protobuf.Timestamp

data class MessageMetadata(
    val messageId: MessageID,
    val timestamp: Timestamp,
    val messageType: String?
)