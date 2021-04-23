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

package com.exactpro.th2.codec.util

fun Exception.getAllMessages(): String = getMessage(this)

private fun getMessage(exception: Throwable?): String {
    return when {
        exception?.cause != null && exception.cause != exception ->
            "${exception.message}. Caused by: ${getMessage(exception.cause)}"
        exception?.message != null ->
            "${exception.message}."
        else -> ""
    }
}