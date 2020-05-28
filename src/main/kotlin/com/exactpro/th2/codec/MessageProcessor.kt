package com.exactpro.th2.codec

interface MessageProcessor<T, R> {
    fun process(source : T): R
}