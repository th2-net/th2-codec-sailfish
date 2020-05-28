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
        logger.debug { "start filtering batch ${sourceBatch.toDebugString()} by filter '${filter.javaClass.simpleName}'" }
        val filteredBatch = sourceBatch.filterNested(filter)
        logger.debug { "filtered ${filteredBatch.toDebugString()}" }
        if (filteredBatch.isEmpty()) {
            logger.warn { "batch is empty after filtering" }
        } else {
            try {
                channel.basicPublish(exchangeName, routingKey, null, filteredBatch.toByteArray())
                logger.info { " batch was sent successfully to '$exchangeName':'$routingKey'" }
            } catch (exception: IOException) {
                logger.error(exception) { "could not send batch to '$exchangeName':'$routingKey'" }
            }
        }
    }
}