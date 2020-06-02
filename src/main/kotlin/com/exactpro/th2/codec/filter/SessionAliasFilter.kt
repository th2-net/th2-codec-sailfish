package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters
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

    companion object {
        private const val SESSION_ALIAS = "sessionAlias"
    }
}