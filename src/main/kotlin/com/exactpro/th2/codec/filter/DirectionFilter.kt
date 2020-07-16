/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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