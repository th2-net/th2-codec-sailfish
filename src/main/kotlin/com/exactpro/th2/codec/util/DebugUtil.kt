package com.exactpro.th2.codec.util

import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.util.JsonFormat

fun GeneratedMessageV3.toDebugString(): String {
    return JsonFormat.printer().omittingInsignificantWhitespace().print(this)
}

fun ByteArray.toHexString() = joinToString("") { "%02x".format(it) }