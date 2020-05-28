package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.Filter

interface CommonBatch {
    fun filterNested(filter: Filter): CommonBatch
    fun isEmpty(): Boolean
    fun toByteArray(): ByteArray
    fun toDebugString(): String
}