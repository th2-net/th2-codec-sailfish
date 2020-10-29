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

import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.Value
import java.util.regex.Pattern

class MessageFilter(parameterName: String, parameterValue: Any) : Filter {

    private val fieldPatternMap: Map<String, Pattern>

    init {
        if (parameterValue !is Map<*,*>) {
            throw IllegalArgumentException("$parameterName must be instance of Map<String, String>")
        }
        fieldPatternMap = parameterValue
            .mapKeys { it.toString() }
            .mapValues { Pattern.compile(it.value.toString()) }
    }

    override fun filter(input: FilterInput): Boolean {
        if (input.message == null) {
            throw IllegalArgumentException("message is required for current message filter")
        }
        return checkFields(input.message.fieldsMap, mutableSetOf())
    }

    private fun checkFields(fields: Map<String, Value>, matchedValues: MutableSet<String> ): Boolean {
        for (field in fields) {
            if (checkValue(field.key, field.value, matchedValues)) {
                return true
            }
        }
        return false
    }

    private fun checkValue(fieldName: String, value: Value, matchedValues: MutableSet<String>) = when {
        value.hasMessageValue() -> checkFields(value.messageValue.fieldsMap, matchedValues)
        value.hasListValue() -> checkListValue(fieldName, value.listValue, matchedValues)
        else -> checkSimple(fieldName, value.simpleValue, matchedValues)
    }

    private fun checkSimple(fieldName: String, value: String, matchedValues: MutableSet<String>): Boolean {
        val pattern = fieldPatternMap[fieldName]
        if (pattern != null &&  pattern.matcher(value).matches()) {
            matchedValues.add(fieldName)
            return fieldPatternMap.keys.containsAll(matchedValues)
        }
        return false
    }

    private fun checkListValue(fieldName: String, listValue: ListValue, matchedValues: MutableSet<String>): Boolean {
        for (value in listValue.valuesList) {
            if (checkValue(fieldName, value, matchedValues)) {
                return true
            }
        }
        return false
    }
}