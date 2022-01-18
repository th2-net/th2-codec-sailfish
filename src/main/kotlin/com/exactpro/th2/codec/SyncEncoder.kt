/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.schema.message.MessageRouter

class SyncEncoder(
    router: MessageRouter<MessageGroupBatch>,
    applicationContext: ApplicationContext,
    private val processor: AbstractCodecProcessor<Message, RawMessage.Builder>
) : AbstractSyncCodec(
    router,
    applicationContext
) {
    override fun getDirection(): Direction = Direction.ENCODE


    override fun checkResultBatch(resultBatch: MessageGroupBatch): Boolean = resultBatch.groupsCount > 0

    override fun checkResult(protoResult: MessageGroup): Boolean = protoResult.messagesCount > 0

    override fun processMessageGroup(it: MessageGroup): MessageGroup? {
        if (it.messagesCount < 1) {
            return null
        }

        val groupBuilder = MessageGroup.newBuilder()

        for (notTypeMessage in it.messagesList) {
            if (notTypeMessage.hasMessage()) {
                val message = notTypeMessage.message
                if (checkProtocol(message)) {
                    groupBuilder.addMessages(AnyMessage.newBuilder().setRawMessage(processor.process(message)).build())
                    continue
                }
            }
            groupBuilder.addMessages(notTypeMessage)
        }

        return groupBuilder.build()
    }

    private fun checkProtocol(message: Message) = message.metadata.protocol.let {
        it.isNullOrEmpty() || it == processor.protocol
    }
}

