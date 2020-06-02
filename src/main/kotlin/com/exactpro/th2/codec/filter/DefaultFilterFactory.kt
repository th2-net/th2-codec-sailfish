package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters

class DefaultFilterFactory : FilterFactory {

    override fun create(parameters: FilterParameters): Filter {
        return when (parameters.filterType) {
            DIRECTION_TYPE -> DirectionFilter(parameters)
            ANY_TYPE -> AnyFilter()
            else -> throw IllegalArgumentException("unknown filter type '${parameters.filterType}'")
        }
    }

    companion object {
        private const val DIRECTION_TYPE = "direction"
        private const val ANY_TYPE = "any"
    }
}