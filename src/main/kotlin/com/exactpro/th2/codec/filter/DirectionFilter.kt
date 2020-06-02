package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters
import com.exactpro.th2.infra.grpc.Direction
import java.util.*

class DirectionFilter(filterParameters: FilterParameters) : Filter {

    private val directions: EnumSet<Direction>

    init {
        val directionsValue = filterParameters.parameters
            ?: throw IllegalArgumentException("'$DIRECTIONS_KEY' parameter is missing")
        if (directionsValue !is List<*>) {
            throw IllegalArgumentException("'$DIRECTIONS_KEY' parameter is not string array")
        }
        directions = EnumSet.copyOf((directionsValue as List<*>).map { Direction.valueOf(it.toString()) })
    }

    override fun filter(input: FilterInput): Boolean {
        return directions.contains(input.messageMetadata.messageId.direction)
    }

    companion object {
        private const val DIRECTIONS_KEY = "directions"
    }
}