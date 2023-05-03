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

import com.exactpro.sf.common.messages.FieldMetaData
import com.exactpro.sf.common.messages.IFieldInfo
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.MsgMetaData
import com.exactpro.sf.common.messages.namespace
import com.exactpro.sf.common.messages.structures.IFieldStructure
import com.exactpro.sf.common.messages.structures.IMessageStructure
import com.exactpro.th2.codec.Th2IMessage
import com.exactpro.th2.codec.util.WrapperFieldInfo
import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Message.Builder
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.sailfish.utils.IMessageToProtoConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter


class ProtoIMessage(
    private val builder: Builder = Message.newBuilder(),
    private val metadata: MsgMetaData,
    private val msgStructure: IMessageStructure,
    private val toProtoConverter: IMessageToProtoConverter,
    private val fromProtoConverter: ProtoToIMessageConverter,
    private val messageFactory: ProtoIMessageFactory
): Th2IMessage<Builder> {

    private val fieldMetadataMap = mutableMapOf<String, FieldMetaData>()

    override fun getMessage(): Builder = builder

    override fun getName(): String = metadata.msgName

    override fun getNamespace(): String = metadata.namespace

    override fun getMetaData(): MsgMetaData = metadata

    override fun addField(name: String?, value: Any?) {
        if (name == null || value == null) return
        builder.addField(name, value.convertToValue())
    }

    override fun removeField(name: String?): Any? {
        if(name == null) return null
        val value = builder.fieldsMap[name] ?: return null
        val structure = msgStructure.fields[name] ?: return null
        builder.removeFields(name)
        return value.convertFromValue(structure)
    }

    override fun <T : Any?> getField(name: String?): T? {
        if(name == null) return null
        val value = builder.fieldsMap[name] ?: return null
        val structure = msgStructure.fields[name] ?: return null

        return value.convertFromValue(structure) as T?
    }

    override fun getFieldMetaData(name: String): FieldMetaData? {
        if(!fieldMetadataMap.containsKey(name)) fieldMetadataMap[name] = FieldMetaData()
        return fieldMetadataMap[name]
    }

    override fun isFieldSet(name: String?): Boolean {
        return builder.fieldsMap[name ?: return false] != null
    }

    override fun getFieldNames(): MutableSet<String> = builder.fieldsMap.keys.toMutableSet()

    override fun getFieldCount(): Int = builder.fieldsCount

    override fun getFieldInfo(name: String?): IFieldInfo? {
        return WrapperFieldInfo(
            name ?: return null,
            fromProtoConverter.convertFromValue(
                builder.fieldsMap[name] ?: return null,
        msgStructure.fields[name] ?: return null
            )
        )
    }

    override fun cloneMessage(): IMessage = ProtoIMessage(
        builder.clone(),
        metadata,
        msgStructure,
        toProtoConverter,
        fromProtoConverter,
        messageFactory
    )

    private fun Value.convertFromValue(structure: IFieldStructure): Any? {
        if(hasMessageValue()) {
            return messageFactory.createMessage(messageValue.newBuilderForType())
        }
        if(hasListValue()
            && listValue.valuesList.isNotEmpty()
            || listValue.valuesList.first().hasMessageValue()
        ) {
            return listValue.valuesList.toMutableList().map {
                messageFactory.createMessage(messageValue.newBuilderForType())
            }
        }

        return fromProtoConverter.convertFromValue(this, structure)
    }

    private fun Any.convertToValue(): Value {
        if(this is Th2IMessage<*>) {
            return Value.newBuilder()
                .setMessageValue(this.getMessage() as Builder)
                .build()
        }

        if(this is List<*> && this.isNotEmpty() && this.first() is Th2IMessage<*>) {
            return Value.newBuilder()
                .setListValue(
                    ListValue.newBuilder()
                        .addAllValues(
                            this.toMutableList().map {
                                Value.newBuilder().setMessageValue((it as Th2IMessage<Message.Builder>).getMessage()).build()
                            }
                        )
                )
                .build()
        }

        return toProtoConverter.convertToValue(this)
    }

    override fun compare(message: IMessage?): Boolean = false
}