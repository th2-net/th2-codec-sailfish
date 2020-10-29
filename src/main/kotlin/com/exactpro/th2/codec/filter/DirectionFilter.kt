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

import com.exactpro.th2.common.grpc.Direction
import java.util.EnumSet

class DirectionFilter(parameterName: String, directionsValue: Any) : Filter {

    private val directions: EnumSet<Direction>

    init {
        if (directionsValue !is List<*>) {
            throw IllegalArgumentException(
                "parameters is not string array for '$parameterName'")
        }
        directions = EnumSet.copyOf(directionsValue.map { Direction.valueOf(it.toString()) })
    }

    override fun filter(input: FilterInput): Boolean {
        return directions.contains(input.messageMetadata.messageId.direction)
    }
}