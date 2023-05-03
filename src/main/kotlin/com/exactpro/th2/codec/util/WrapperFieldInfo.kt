/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec.util

import com.exactpro.sf.common.messages.IFieldInfo
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.util.EPSCommonException
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

data class WrapperFieldInfo(
    private val name: String,
    private val value: Any?
): IFieldInfo {

    override fun getFieldType(): IFieldInfo.FieldType = convert(value)

    override fun isCollection(): Boolean = value is Array<*> || value is Iterable<*>

    override fun getName(): String = name

    override fun getValue(): Any? = value

    companion object {
        fun convert(value: Any?): IFieldInfo.FieldType {
            if (value == null) {
                throw NullPointerException("$value")
            }

            if (value is IMessage) {
                return IFieldInfo.FieldType.SUBMESSAGE
            }

            if (value is Boolean || value is BooleanArray) {
                return IFieldInfo.FieldType.BOOLEAN
            }

            if (value is Short || value is ShortArray) {
                return IFieldInfo.FieldType.SHORT
            }

            if (value is Int || value is IntArray) {
                return IFieldInfo.FieldType.INT
            }

            if (value is Long || value is LongArray) {
                return IFieldInfo.FieldType.LONG
            }

            if (value is Byte || value is ByteArray) {
                return IFieldInfo.FieldType.BYTE
            }

            if (value is Float || value is FloatArray) {
                return IFieldInfo.FieldType.FLOAT
            }

            if (value is Double || value is DoubleArray) {
                return IFieldInfo.FieldType.DOUBLE
            }

            if (value is String || (value is Array<*> && value.isArrayOf<String>())) {
                return IFieldInfo.FieldType.STRING
            }

            if (value is LocalDateTime || (value is Array<*> && value.isArrayOf<LocalDateTime>())) {
                return IFieldInfo.FieldType.DATE_TIME
            }

            if (value is LocalDate || (value is Array<*> && value.isArrayOf<LocalDate>())) {
                return IFieldInfo.FieldType.DATE
            }

            if (value is LocalTime || (value is Array<*> && value.isArrayOf<LocalTime>())) {
                return IFieldInfo.FieldType.TIME
            }

            if (value is Char || value is CharArray) {
                return IFieldInfo.FieldType.CHAR
            }

            if (value is BigDecimal || (value is Array<*> && value.isArrayOf<BigDecimal>())) {
                return IFieldInfo.FieldType.DECIMAL
            }

            throw EPSCommonException("Cannot associate  [" + value.javaClass.canonicalName + "] with FieldType")
        }
    }

}