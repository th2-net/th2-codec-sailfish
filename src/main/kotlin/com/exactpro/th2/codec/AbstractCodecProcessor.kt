/*
 *  Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec

import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings

abstract class AbstractCodecProcessor<B, M, R>(
    private val codecFactory: IExternalCodecFactory,
    private val codecSettings: IExternalCodecSettings,
    private val messageFactory: Th2MessageFactory<*>
) : MessageProcessor<B, M, R> {

    private val codecThreadInstances = object : ThreadLocal<IExternalCodec>() {
        override fun initialValue(): IExternalCodec {
            return codecFactory.createCodec(codecSettings)
        }
    }

    protected fun getCodec(): IExternalCodec {
        return codecThreadInstances.get()
    }
}