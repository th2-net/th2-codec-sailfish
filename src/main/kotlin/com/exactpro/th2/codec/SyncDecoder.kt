/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.MessageRouter

class SyncDecoder(
    router: MessageRouter<MessageGroupBatch>,
    applicationContext: ApplicationContext,
    private val processor: AbstractCodecProcessor<RawMessage, Message.Builder?>,
    codecRootID: EventID?
): AbstractSyncCodec(
    router,
    applicationContext,
    codecRootID
) {

    companion object {
        const val DEFAULT_SUBSEQUENCE_NUMBER = 1
    }

    override fun getParentEventId(
        codecRootID: EventID?,
        protoSource: MessageGroup?,
        protoResult: MessageGroup?
    ): EventID? {
        return codecRootID
    }

    override fun checkResult(protoResult: MessageGroup): Boolean = protoResult.messagesCount > 0

    override fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean = resultBatch.groupsCount > 0

    override fun processMessageGroup(it: MessageGroup): MessageGroup? {
        if (it.messagesCount < 1) {
            return null
        }

        val groupBuilder = MessageGroup.newBuilder()

        for (notTypeMessage in it.messagesList) {
            if (notTypeMessage.hasRawMessage()) {
                val rawMessage = notTypeMessage.rawMessage
                processor.process(rawMessage)?.apply {
                    metadataBuilder.idBuilder.addSubsequence(DEFAULT_SUBSEQUENCE_NUMBER)
                    groupBuilder.addMessages(AnyMessage.newBuilder().setMessage(this))
                }
            } else {
                groupBuilder.addMessages(notTypeMessage)
            }
        }

        return if (groupBuilder.messagesCount > 0) groupBuilder.build() else null
    }

}