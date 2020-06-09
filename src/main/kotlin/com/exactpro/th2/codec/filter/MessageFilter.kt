package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters
import com.exactpro.th2.codec.filter.DefaultFilterFactory.Companion.FIELD_VALUES
import com.exactpro.th2.infra.grpc.ListValue
import com.exactpro.th2.infra.grpc.Value
import java.util.regex.Pattern

class MessageFilter(filterParameters: FilterParameters) : Filter {

    private val fieldPatternMap: Map<String, Pattern>

    init {
        val params: Map<String, String> = filterParameters.parameters
            ?: throw IllegalArgumentException("filter parameters is missing")
        fieldPatternMap = params.mapValues { Pattern.compile(it.value) }
    }

    override fun filter(input: FilterInput): Boolean {
        if (input.message == null) {
            throw IllegalArgumentException("message is required for current message filter")
        }
        return checkFields(input.message.fieldsMap, mutableSetOf())
    }

    override fun getType(): String = FIELD_VALUES

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