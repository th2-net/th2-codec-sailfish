package com.exactpro.th2.codec.filter

import com.exactpro.th2.infra.grpc.Message

class FilterInput(
    val messageMetadata: MessageMetadata,
    val message: Message?
)