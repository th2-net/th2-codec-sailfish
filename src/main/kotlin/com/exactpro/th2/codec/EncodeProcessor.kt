package com.exactpro.th2.codec

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.infra.grpc.*
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging

class EncodeProcessor(
    private val codec: IExternalCodec,
    private val converter: ProtoToIMessageConverter
) : MessageProcessor<MessageBatch, RawMessageBatch> {

    private val logger = KotlinLogging.logger {  }

    override fun process(source: MessageBatch): RawMessageBatch {
        val rawMessageBatchBuilder = RawMessageBatch.newBuilder()
        for (protoMessage in source.messagesList) {
            val convertedSourceMessage = converter.fromProtoMessage(protoMessage, true).also {
                logger.debug { "converted source message '${it.name}': $it" }
            }
            val encodedMessageData = codec.encode(convertedSourceMessage)
            rawMessageBatchBuilder.addMessages(RawMessage.newBuilder()
                .setBody(ByteString.copyFrom(encodedMessageData))
                .setMetadata(toRawMessageMetadataBuilder(protoMessage).also {
                    logger.debug {
                        val jsonRawMessage = JsonFormat.printer().omittingInsignificantWhitespace().print(it)
                        "message metadata: $jsonRawMessage"
                    }
                })
            )
        }
        return rawMessageBatchBuilder.build()
    }

    private fun toRawMessageMetadataBuilder(sourceMessage: Message): RawMessageMetadata {
        return RawMessageMetadata.newBuilder()
            .setId(sourceMessage.metadata.id)
            .setTimestamp(sourceMessage.metadata.timestamp)
            .build()
    }
}