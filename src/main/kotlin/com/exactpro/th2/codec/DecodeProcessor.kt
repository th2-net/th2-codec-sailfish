package com.exactpro.th2.codec

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.codec.util.toCodecContext
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import mu.KotlinLogging

class DecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    messageToProtoConverter: IMessageToProtoConverter
) : RawDecodeProcessor(codecFactory, codecSettings, messageToProtoConverter) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: RawMessage): List<Message.Builder> {
        val processSingle = processSingle(source)
        return if (processSingle != null) {
            listOf(processSingle)
        } else {
            emptyList()
        }
    }

    private fun processSingle(rawMessage: RawMessage): Message.Builder? {
        try {
            val data: ByteArray = rawMessage.body.toByteArray()
            logger.debug { "Decoding message: ${rawMessage.toDebugString()}" }
            val decodedMessages = getCodec().decode(data, rawMessage.toCodecContext())
            logger.debug { "Decoded messages: $decodedMessages" }
            val decodedMessage: IMessage = checkCountAndRawData(decodedMessages, data)

            val messageMetadata = toMessageMetadataBuilder(rawMessage)
                .setMessageType(decodedMessage.name)
                .build()
            return messageToProtoConverter.toProtoMessage(decodedMessage)
                .setMetadata(messageMetadata)
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $rawMessage" }
            return null
        }
    }

    /**
     * Checks that the [decodedMessages] contains exact one message and its raw data is the same as [originalData].
     */
    private fun checkCountAndRawData(decodedMessages: List<IMessage>, originalData: ByteArray): IMessage {
        val decodedMessage = when {
            decodedMessages.size == 1 -> decodedMessages[0]
            decodedMessages.isEmpty() -> throw DecodeException("No message was decoded")
            else -> throw DecodeException("More than one message was decoded: ${decodedMessages.size}")
        }
        val rawMessage = decodedMessage.metaData.rawMessage
            ?: throw DecodeException("Raw data is null for message: ${decodedMessage.name}")
        return if (rawMessage contentEquals originalData) {
            decodedMessage
        } else {
            throw DecodeException("The decoded raw data is different from the original one. " +
                    "Decoded: ${rawMessage.contentToString()}, Original: ${originalData.contentToString()}")
        }
    }

}