package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.filter.DefaultFilterFactory.Companion.ANY_TYPE

class AnyFilter : Filter {
    override fun filter(input: FilterInput) = true
    override fun getType(): String  = ANY_TYPE
}