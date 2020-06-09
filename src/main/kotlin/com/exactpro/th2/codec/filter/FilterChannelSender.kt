package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.CommonBatch
import com.rabbitmq.client.Channel
import mu.KotlinLogging
import java.io.IOException

class FilterChannelSender(
    private val channel: Channel,
    private val filter: Filter,
    private val exchangeName: String,
    private val routingKey: String
) {
    private val logger = KotlinLogging.logger {  }

    fun filterAndSend(sourceBatch: CommonBatch) {
        logger.debug { "${channelInfo()}: start filtering batch ${sourceBatch.toDebugString()}" }
        val filteredBatch = sourceBatch.filterNested(filter)
        if (filteredBatch.isEmpty()) {
            logger.warn { "${channelInfo()}: batch is empty after filtering" }
        } else {
            try {
                logger.debug { "${channelInfo()}: filtered ${filteredBatch.toDebugString()}" }
                channel.basicPublish(exchangeName, routingKey, null, filteredBatch.toByteArray())
                logger.info { "${channelInfo()}:  batch was sent successfully to '$exchangeName':'$routingKey'" }
            } catch (exception: IOException) {
                logger.error(exception) { "could not send batch to '$exchangeName':'$routingKey'" }
            }
        }
    }

    private fun channelInfo(): String  = "${filter.getType()}filter('$exchangeName':'$routingKey')"
}