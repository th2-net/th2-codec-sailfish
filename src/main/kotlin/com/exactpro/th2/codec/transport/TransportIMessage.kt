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
package com.exactpro.th2.codec.transport

import com.exactpro.sf.common.messages.FieldMetaData
import com.exactpro.sf.common.messages.IFieldInfo
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.MsgMetaData
import com.exactpro.sf.common.messages.name
import com.exactpro.sf.common.messages.namespace
import com.exactpro.sf.common.messages.structures.IFieldStructure
import com.exactpro.sf.common.messages.structures.IMessageStructure
import com.exactpro.th2.codec.Th2IMessage
import com.exactpro.th2.codec.util.WrapperFieldInfo
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.sailfish.utils.transport.IMessageToTransportConverter
import com.exactpro.th2.sailfish.utils.transport.TransportToIMessageConverter

class TransportIMessage(
    private val message: ParsedMessage = ParsedMessage.newMutable().also { it.type = metadata.name },
    private val metadata: MsgMetaData,
    private val msgStructure: IMessageStructure,
    private val toTransportConverter: IMessageToTransportConverter,
    private val fromTransportConverter: TransportToIMessageConverter,
    private val messageFactory: TransportIMessageFactory
): Th2IMessage<ParsedMessage> {

    private val fieldMetadataMap = mutableMapOf<String, FieldMetaData>()

    private val body = message.body

    override fun getMessage(): ParsedMessage = message

    override fun getName(): String = metadata.msgName

    override fun getNamespace(): String = metadata.namespace

    override fun getMetaData(): MsgMetaData = metadata

    override fun addField(name: String?, value: Any?) {
        if(name == null || value == null) return
        body[name] = value.convertToTransportValue()
    }

    override fun removeField(name: String?): Any? {
        if(name == null) return null
        val structure = msgStructure.fields[name] ?: return null
        val value = body.remove(name) ?: return null
        return value.convertFromTransportValue(structure)
    }

    override fun <T : Any?> getField(name: String?): T? {
        if(name == null) return null
        val structure = msgStructure.fields[name] ?: return null
        val value = body.remove(name) ?: return null
        return fromTransportConverter.fromTransport(value, structure) as T?
    }

    override fun getFieldMetaData(name: String): FieldMetaData? {
        if(!fieldMetadataMap.containsKey(name)) fieldMetadataMap[name] = FieldMetaData()
        return fieldMetadataMap[name]
    }

    override fun isFieldSet(name: String?): Boolean = body.contains(name)

    override fun getFieldNames(): MutableSet<String> = body.keys.toMutableSet()

    override fun getFieldCount(): Int = body.size

    override fun getFieldInfo(name: String?): IFieldInfo? {
        return WrapperFieldInfo(
            name ?: return null,
            fromTransportConverter.fromTransport(
                body[name] ?: return null,
                msgStructure.fields[name] ?: return null
            )
        )
    }

    override fun cloneMessage(): IMessage = TransportIMessage(
        ParsedMessage.newMutable().also {
            it.type = metadata.name
            it.body = body
        },
        metadata,
        msgStructure,
        toTransportConverter,
        fromTransportConverter,
        messageFactory
    )

    override fun compare(message: IMessage?): Boolean = false

    private fun Any.convertFromTransportValue(structure: IFieldStructure): Any? {
        if(structure.isCollection && structure.isComplex) {
            return (this as List<MutableMap<String, Any>>).toMutableList().map {
                messageFactory.createMessage(
                    ParsedMessage.newMutable().apply {
                        body = it
                        type = structure.name
                    }
                )
            }
        }

        if(structure.isComplex) {
            (this as MutableMap<String, Any>).let {
                return messageFactory.createMessage(
                    ParsedMessage.newMutable().apply {
                        body = it
                        type = structure.name
                    }
                )
            }
        }

        return fromTransportConverter.fromTransport(this, structure)
    }

    private fun Any.convertToTransportValue(): Any {
        if(this is Th2IMessage<*>) {
            return (this.getMessage() as ParsedMessage).body
        }

        if(this is List<*> && this.isNotEmpty() && this.first() is Th2IMessage<*>) {
            val result = mutableListOf<Map<String, Any>>()
            this.forEach { result.add((it as Th2IMessage<ParsedMessage>).getMessage().body) }
            return result
        }

        val value = this
        return toTransportConverter.apply { value.toTransport() }
    }
}