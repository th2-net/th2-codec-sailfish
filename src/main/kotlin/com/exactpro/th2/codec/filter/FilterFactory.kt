package com.exactpro.th2.codec.filter

import com.exactpro.th2.codec.configuration.FilterParameters

interface FilterFactory {
    fun create(parameters: FilterParameters): Filter
}