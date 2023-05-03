/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec.proto

import com.exactpro.sf.common.impl.messages.AbstractMessageFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.MsgMetaData
import com.exactpro.sf.common.messages.messageProperties
import com.exactpro.sf.common.messages.structures.DictionaryConstants.ATTRIBUTE_IS_ADMIN
import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.StructureUtils.getAttributeValue
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.th2.codec.Th2MessageFactory
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter


class ProtoIMessageFactory(
    private val _protocol: String,
    private val toProtoConverter: IMessageToProtoConverter,
    private val fromProtoConverter: ProtoToIMessageConverter
): AbstractMessageFactory(), Th2MessageFactory<Message.Builder> {
    private lateinit var dictionary: IDictionaryStructure
    override fun getProtocol(): String = _protocol

    override fun init(dictionaryURI: SailfishURI?, dictionary: IDictionaryStructure?) {
        super.init(dictionaryURI, dictionary)
        this.dictionary = requireNotNull(dictionary)
    }

    override fun createMessage(metaData: MsgMetaData): IMessage? {
        val messageStructure = requireNotNull(dictionary.messages[metaData.msgName]) {
            "Not found message ${metaData.msgName} in dictionary ${dictionary.namespace}"
        }
        if (metaData.msgNamespace == namespace) {
            metaData.dictionaryURI = dictionaryURI
            metaData.protocol = protocol
            if (messageStructure != null) {
                val isAdmin: Boolean? = getAttributeValue(messageStructure, ATTRIBUTE_IS_ADMIN)
                metaData.isAdmin = isAdmin ?: false
            }
        }
        val message: IMessage = ProtoIMessage(
            metadata = metaData,
            msgStructure = messageStructure,
            fromProtoConverter = fromProtoConverter,
            toProtoConverter = toProtoConverter,
            messageFactory = this
        )
        return message
    }

    override fun createMessage(content: Message.Builder): IMessage {
        val metaData = MsgMetaData(content.messageType, namespace)
        val messageStructure = requireNotNull(dictionary.messages[metaData.msgName]) {
            "Not found message ${metaData.msgName} in dictionary ${dictionary.namespace}"
        }
        if (metaData.msgNamespace == namespace) {
            metaData.dictionaryURI = dictionaryURI
            metaData.protocol = protocol
            if (messageStructure != null) {
                val isAdmin: Boolean? = getAttributeValue(messageStructure, ATTRIBUTE_IS_ADMIN)
                metaData.isAdmin = isAdmin ?: false
            }
        }

        if (content.metadata.propertiesMap.isNotEmpty()) {
            metaData.messageProperties = content.metadata.propertiesMap
        }

        val message: IMessage = ProtoIMessage(
            content,
            metaData,
            messageStructure,
            toProtoConverter,
            fromProtoConverter,
            this
        )
        return message
    }
}