package com.exactpro.th2.codec

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings

abstract class AbstractCodecProcessor<T, R>(
    private val codecFactory: IExternalCodecFactory,
    private val codecSettings: IExternalCodecSettings
) : MessageProcessor<T, R> {

    private val codecThreadInstances = object : ThreadLocal<IExternalCodec>() {
        override fun initialValue(): IExternalCodec {
            return codecFactory.createCodec(codecSettings)
        }
    }

    protected fun getCodec(): IExternalCodec {
        return codecThreadInstances.get()
    }
}