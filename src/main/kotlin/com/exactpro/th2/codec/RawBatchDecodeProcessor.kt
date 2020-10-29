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

package com.exactpro.th2.codec

import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter

abstract class RawBatchDecodeProcessor(
    codecFactory: IExternalCodecFactory,
    codecSettings: IExternalCodecSettings,
    protected val messageToProtoConverter: IMessageToProtoConverter
) : AbstractCodecProcessor<RawMessageBatch, MessageBatch>(codecFactory, codecSettings) {

    protected fun toMessageMetadataBuilder(sourceMessage: RawMessage): MessageMetadata.Builder {
        return MessageMetadata.newBuilder()
            .setId(sourceMessage.metadata.id)
            .setTimestamp(sourceMessage.metadata.timestamp)
            .putAllProperties(sourceMessage.metadata.propertiesMap)
    }
}