package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.FilterParameters

interface FilterFactory {
    fun create(parameters: FilterParameters): Filter
}