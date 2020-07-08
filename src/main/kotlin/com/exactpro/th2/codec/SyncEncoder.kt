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

import com.exactpro.th2.codec.EncodeMessageSender.EncodeCommonBatch
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.CodecParameters
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessageBatch

class SyncEncoder(
    codecParameters: CodecParameters,
    applicationContext: ApplicationContext,
    processor: AbstractCodecProcessor<MessageBatch, RawMessageBatch>,
    codecRootID: EventID?
): AbstractSyncCodec<MessageBatch, RawMessageBatch>(
    codecParameters,
    applicationContext,
    processor,
    codecRootID
) {
    override fun getParentEventId(
        codecRootID: EventID?,
        protoSource: MessageBatch?,
        protoResult: RawMessageBatch?
    ): EventID? {
        if (protoSource != null && protoSource.messagesCount != 0
            && protoSource.getMessages(0).hasParentEventId()) {
            return protoSource.getMessages(0).parentEventId
        }
        return codecRootID
    }

    override fun parseProtoSourceFrom(data: ByteArray): MessageBatch = MessageBatch.parseFrom(data)

    override fun checkResult(protoResult: RawMessageBatch): Boolean = protoResult.messagesCount != 0

    override fun toCommonBatch(protoResult: RawMessageBatch): CommonBatch = EncodeCommonBatch(protoResult)
}