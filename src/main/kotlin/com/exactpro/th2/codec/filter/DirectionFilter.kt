package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters
import com.exactpro.th2.codec.filter.DefaultFilterFactory.Companion.DIRECTION_TYPE
import com.exactpro.th2.infra.grpc.Direction
import java.util.*

class DirectionFilter(filterParameters: FilterParameters) : Filter {

    private val directions: EnumSet<Direction>

    init {
        val directionsValue = filterParameters.parameters ?: throw IllegalArgumentException(
            " parameters is missing for ${filterParameters.filterType}:${filterParameters.queueName}")
        if (directionsValue !is List<*>) {
            throw IllegalArgumentException(
                "parameters is not string array for ${filterParameters.filterType}:${filterParameters.queueName}\"")
        }
        directions = EnumSet.copyOf((directionsValue as List<*>).map { Direction.valueOf(it.toString()) })
    }

    override fun filter(input: FilterInput): Boolean {
        return directions.contains(input.messageMetadata.messageId.direction)
    }

    override fun getType(): String  = DIRECTION_TYPE
}