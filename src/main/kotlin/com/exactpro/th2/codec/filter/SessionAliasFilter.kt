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
import com.exactpro.th2.codec.filter.DefaultFilterFactory.Companion.SESSION_ALIAS
import java.util.regex.Pattern

class SessionAliasFilter(filterParameters: FilterParameters) : Filter {

    private val value: Pattern

    init {
        val parameters = filterParameters.parameters
            ?: throw IllegalArgumentException("parameters is missing")
        val patternValue = parameters[SESSION_ALIAS]
            ?: throw IllegalArgumentException("'$SESSION_ALIAS' parameter is missing")
        value = Pattern.compile(patternValue)
    }

    override fun filter(input: FilterInput): Boolean {
        return value.matcher(input.messageMetadata.messageId.connectionId.sessionAlias).matches()
    }

    override fun getType(): String = SESSION_ALIAS
}