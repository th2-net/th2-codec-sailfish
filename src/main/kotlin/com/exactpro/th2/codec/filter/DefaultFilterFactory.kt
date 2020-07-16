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

class DefaultFilterFactory : FilterFactory {

    override fun create(parameters: FilterParameters): Filter {
        val filters = mutableListOf<Filter>()
        when {
            parameters.directions != null -> filters.add(
                DirectionFilter(DIRECTION_TYPE, parameters.directions!!))
            parameters.sessionAlias != null -> filters.add(
                SessionAliasFilter(SESSION_ALIAS, parameters.sessionAlias!!))
            parameters.messageType != null -> filters.add(
                MessageTypeFilter(MESSAGE_TYPE, parameters.messageType!!))
            parameters.fieldValues != null -> filters.add(
                MessageFilter(FIELD_VALUES, parameters.fieldValues!!))
        }
        return CompositeFilter(filters)
    }

    companion object {
        const val DIRECTION_TYPE = "direction"
        const val SESSION_ALIAS = "sessionAlias"
        const val MESSAGE_TYPE = "messageType"
        const val FIELD_VALUES = "fieldValues"
    }
}