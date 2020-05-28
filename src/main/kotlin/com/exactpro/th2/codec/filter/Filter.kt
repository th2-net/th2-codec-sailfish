package com.exactpro.th2.codec.filter

interface Filter {
    fun filter(input: FilterInput): Boolean
}