/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.sailfish

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory.getFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.common.util.ICommonSettings
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.DictionaryProperty
import com.exactpro.sf.externalapi.DictionaryType.LEVEL1
import com.exactpro.sf.externalapi.DictionaryType.MAIN
import com.exactpro.sf.externalapi.codec.IExternalCodec
import com.exactpro.sf.externalapi.codec.IExternalCodecFactory
import com.exactpro.sf.externalapi.codec.IExternalCodecSettings
import com.exactpro.sf.externalapi.codec.impl.ExternalCodecSettings
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.api.impl.PipelineCodecContext
import com.exactpro.th2.codec.sailfish.SailfishCodecFactory.Companion.fillSettings
import com.exactpro.th2.codec.sailfish.SailfishCodecFactory.Companion.loadDefaultCodecParameters
import com.exactpro.th2.codec.sailfish.configuration.SailfishConfiguration
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.fasterxml.jackson.core.type.TypeReference
import com.google.auto.service.AutoService
import com.google.protobuf.UnsafeByteOperations
import org.apache.commons.configuration.HierarchicalConfiguration
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions
import java.time.Instant
import com.exactpro.th2.common.grpc.MessageGroup as ProtobufMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup.Companion as TransportMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage.Companion as TransportRawMessage

private const val PROTOCOL = "test-protocol"
private const val SESSION_ALIAS = "test-session-alias"
private const val BOOK = "test-book"
private const val SCOPE = "test-scope"
private const val EVENT_ID = "test-id"

private const val MESSAGE_TYPE = "test-message-type"
private const val MESSAGE_TYPE_FIELD = "test-message-type-field"
private const val FIELD = "test-field"

class TestSailfishCodecFactory {
    private val mainDictionary =
        """<dictionary xmlns="http://exactprosystems.com/dictionary" name="MAIN">
             <messages>
               <message name="$MESSAGE_TYPE">
                 <field name="$MESSAGE_TYPE_FIELD" type="java.lang.String"/>
                 <field name="$FIELD" type="java.lang.String"/>
               </message>
             </messages>
           </dictionary>""".trimIndent()
    private val level1Dictionary =
        """<dictionary xmlns="http://exactprosystems.com/dictionary" name="LEVEL1"></dictionary>"""

    private val mainDictionaryStructure = mainDictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)
    private val level1DictionaryStructure = level1Dictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)

    private val commonFactory: CommonFactory = mock {
        on { loadDictionary(eq(DictionaryType.MAIN.name)) }.thenReturn(mainDictionary.byteInputStream())
        on { loadDictionary(eq(DictionaryType.LEVEL1.name)) }.thenReturn(level1Dictionary.byteInputStream())
    }

    private val codecSettings = SailfishConfiguration().apply {
        codecClassName = CodecFactory::class.java.name
        dictionaries = mapOf(
            DictionaryType.MAIN.name to DictionaryType.MAIN.name,
        )

    }

    private val codecFactory = SailfishCodecFactory().apply {
        init(PipelineCodecContext(commonFactory))
    }

    private val codec: IPipelineCodec = codecFactory.create(this@TestSailfishCodecFactory.codecSettings)

    private val reportingContext = mock<IReportingContext> { }

    @AfterEach
    fun afterEach() {
        verifyNoInteractions(reportingContext)
    }

    @Test
    fun `load default parameters test`() {
        assertEquals(
            mapOf(
                "fieldByte" to "10",
                "fieldShort" to "10",
                "fieldDouble" to "10.10",
            ),
            loadDefaultCodecParameters()
        )
    }

    @Test
    fun `fill settings test`() {
        val codecContext = mock<IPipelineCodecContext> {
            on { get(eq(DictionaryType.MAIN.name)) }.thenReturn(mainDictionary.byteInputStream())
            on { get(eq(DictionaryType.LEVEL1.name)) }.thenReturn(level1Dictionary.byteInputStream())
        }
        val defaultParameters = mapOf(
            "fieldByte" to "10",
            "fieldShort" to "10",
            "fieldDouble" to "10.10",
        )
        val parameters = mapOf(
            "fieldBoolean" to "true",
            "fieldShort" to "20",
            "fieldLong" to "20",
            "fieldString" to "test",
        )
        val settings = ExternalCodecSettings(Settings()).fillSettings(
            codecContext,
            defaultParameters,
            parameters,
            dictionaries = mapOf(
                DictionaryType.MAIN.name to DictionaryType.MAIN.name,
                DictionaryType.LEVEL1.name to DictionaryType.LEVEL1.name,
            )
        )
        assertEquals(true, settings["fieldBoolean"])
        assertEquals(10.toByte(), settings["fieldByte"])
        assertEquals(20.toShort(), settings["fieldShort"])
        assertEquals(0, settings["fieldInteger"])
        assertEquals(20L, settings["fieldLong"])
        assertEquals(0.0F, settings["fieldFloat"])
        assertEquals(10.10, settings["fieldDouble"])
        assertEquals("test", settings["fieldString"])
        // main dictionary type is replaced because Sailfish codec external API gets main dictionary by type
        assertEquals(SailfishURI.unsafeParse("any"), settings["filedMainDictionary"])
        assertEquals(LEVEL1.toUri(), settings["filedLevel1Dictionary"])
        assertEquals(mainDictionaryStructure.namespace, settings[MAIN]?.namespace)
        assertEquals(level1DictionaryStructure.namespace, settings[LEVEL1]?.namespace)
    }

    @Test
    fun `decode transport test`() {
        val messageId = MessageId.builder()
            .setSequence(1)
            .setDirection(OUTGOING)
            .setTimestamp(Instant.now())
            .setSessionAlias(SESSION_ALIAS)
            .build()

        val eventId = EventId.builder()
            .setBook(BOOK)
            .setScope(SCOPE)
            .setId(EVENT_ID)
            .setTimestamp(Instant.now())
            .build()

        val metadata = mapOf(
            "test-property" to "test-property-value"
        )

        assertEquals(
            TransportMessageGroup.builder()
                .addMessage(
                    ParsedMessage.builder().apply {
                        setId(messageId.toBuilder().addSubsequence(1).build())
                        setEventId(eventId)
                        setMetadata(metadata)
                        setProtocol(PROTOCOL)
                        setType(MESSAGE_TYPE)
                        bodyBuilder().apply {
                            put(MESSAGE_TYPE_FIELD, MESSAGE_TYPE)
                            put(FIELD, "test-value")
                        }
                    }.build()
                ).build(),
            codec.decode(
                TransportMessageGroup.builder()
                    .addMessage(
                        TransportRawMessage.builder().apply {
                            setId(messageId)
                            setEventId(eventId)
                            setMetadata(metadata)
                            setProtocol(PROTOCOL)
                            setBody("""{"$MESSAGE_TYPE_FIELD":"$MESSAGE_TYPE","$FIELD":"test-value"}""".toByteArray())
                        }.build()
                    ).build(), reportingContext
            )
        )
    }

    @Test
    fun `decode proto test`() {
        val messageId = MessageID.newBuilder().apply {
            setBookName(BOOK)
            setSequence(1)
            setDirection(SECOND)
            setTimestamp(Instant.now().toTimestamp())
            connectionIdBuilder
                .setSessionGroup(SESSION_ALIAS)
                .setSessionAlias(SESSION_ALIAS)
        }.build()

        val eventId = EventID.newBuilder()
            .setBookName(BOOK)
            .setScope(SCOPE)
            .setId(EVENT_ID)
            .setStartTimestamp(Instant.now().toTimestamp())
            .build()

        val metadata = mapOf(
            "test-property" to "test-property-value"
        )

        assertEquals(
            ProtobufMessageGroup.newBuilder().apply {
                addMessagesBuilder()
                    .messageBuilder.apply {
                        setParentEventId(eventId)
                        metadataBuilder
                            .setId(messageId.toBuilder().addSubsequence(1).build())
                            .putAllProperties(metadata)
                            .setProtocol(PROTOCOL)
                            .setMessageType(MESSAGE_TYPE)
                        addField(MESSAGE_TYPE_FIELD, MESSAGE_TYPE)
                        addField(FIELD, "test-value")
                    }
            }.build(),
            codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder()
                        .rawMessageBuilder.apply {
                            setParentEventId(eventId)
                            metadataBuilder
                                .setId(messageId)
                                .putAllProperties(metadata)
                                .setProtocol(PROTOCOL)
                            setBody(UnsafeByteOperations.unsafeWrap(
                                """{"$MESSAGE_TYPE_FIELD":"$MESSAGE_TYPE","$FIELD":"test-value"}""".toByteArray()
                            ))
                        }
                }.build(), reportingContext
            )
        )
    }

    @Test
    fun `encode transport test`() {
        val messageId = MessageId.builder()
            .setSequence(1)
            .setDirection(OUTGOING)
            .setTimestamp(Instant.now())
            .setSessionAlias(SESSION_ALIAS)
            .build()

        val eventId = EventId.builder()
            .setBook(BOOK)
            .setScope(SCOPE)
            .setId(EVENT_ID)
            .setTimestamp(Instant.now())
            .build()

        val metadata = mapOf(
            "test-property" to "test-property-value"
        )

        assertEquals(
            TransportMessageGroup.builder()
                .addMessage(
                    TransportRawMessage.builder().apply {
                        setId(messageId)
                        setEventId(eventId)
                        setMetadata(metadata)
                        setProtocol(PROTOCOL)
                        setBody("""{"$MESSAGE_TYPE_FIELD":"$MESSAGE_TYPE","$FIELD":"test-value"}""".toByteArray())
                    }.build()
                ).build(),
            codec.encode(
                TransportMessageGroup.builder()
                    .addMessage(
                        ParsedMessage.builder().apply {
                            setId(messageId)
                            setEventId(eventId)
                            setMetadata(metadata)
                            setProtocol(PROTOCOL)
                            setType(MESSAGE_TYPE)
                            bodyBuilder().apply {
                                put(FIELD, "test-value")
                            }
                        }.build()
                    ).build(), reportingContext
            )
        )
    }

    @Test
    fun `encode proto test`() {
        val messageId = MessageID.newBuilder().apply {
            setBookName(BOOK)
            setSequence(1)
            setDirection(SECOND)
            setTimestamp(Instant.now().toTimestamp())
            connectionIdBuilder
                .setSessionGroup(SESSION_ALIAS)
                .setSessionAlias(SESSION_ALIAS)
        }.build()

        val eventId = EventID.newBuilder()
            .setBookName(BOOK)
            .setScope(SCOPE)
            .setId(EVENT_ID)
            .setStartTimestamp(Instant.now().toTimestamp())
            .build()

        val metadata = mapOf(
            "test-property" to "test-property-value"
        )

        assertEquals(
            ProtobufMessageGroup.newBuilder().apply {
                addMessagesBuilder()
                    .rawMessageBuilder.apply {
                        setParentEventId(eventId)
                        metadataBuilder
                            .setId(messageId)
                            .putAllProperties(metadata)
                            .setProtocol(PROTOCOL)
                        setBody(UnsafeByteOperations.unsafeWrap(
                            """{"$MESSAGE_TYPE_FIELD":"$MESSAGE_TYPE","$FIELD":"test-value"}""".toByteArray()
                        ))
                    }
            }.build(),
            codec.encode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder()
                        .messageBuilder.apply {
                            setParentEventId(eventId)
                            metadataBuilder
                                .setId(messageId)
                                .putAllProperties(metadata)
                                .setProtocol(PROTOCOL)
                                .setMessageType(MESSAGE_TYPE)
                            addField(FIELD, "test-value")
                        }
                }.build(), reportingContext
            )
        )
    }
}

@AutoService(IExternalCodecFactory::class)
internal class CodecFactory : IExternalCodecFactory {
    override val protocolName: String = PROTOCOL

    override fun createCodec(settings: IExternalCodecSettings): IExternalCodec = Codec(settings)
    override fun createSettings(): IExternalCodecSettings = ExternalCodecSettings(SimpleSettings())

    @Deprecated("Set dictionary on an instance instead", replaceWith = ReplaceWith("createSettings()"))
    override fun createSettings(dictionary: IDictionaryStructure): IExternalCodecSettings =
        throw UnsupportedOperationException("'createSettings' method is unsupported")
}

internal class Codec(
    settings: IExternalCodecSettings
) : IExternalCodec {
    private val dictionary: IDictionaryStructure = requireNotNull(settings[MAIN]) {
        "'${MAIN}' dictionary must be set"
    }

    override fun close() {}
    override fun decode(data: ByteArray): List<IMessage> {
        val map = MAPPER.readValue(data, object : TypeReference<Map<String, Any?>>() {})
        val messageType = requireNotNull(map[MESSAGE_TYPE_FIELD]) {
            "'$MESSAGE_TYPE_FIELD' field is empty or null in the incoming message $map"
        }
        check(messageType is String) {
            "'$MESSAGE_TYPE_FIELD' field has incorrect type, expected: ${String::class.java}, actual: ${messageType.javaClass}"
        }
        val structure = requireNotNull(dictionary.messages[messageType]) {
            "Unsupported '$messageType' message type"
        }
        return listOf(
            getFactory().createMessage(messageType, dictionary.namespace).apply {
                metaData.rawMessage = data
                structure.fields.keys.forEach { field ->
                    addField(field, map[field])
                }
            }
        )
    }

    override fun encode(message: IMessage): ByteArray {
        require(message.namespace == dictionary.namespace) {
            "Unsupported namespace, expected: ${dictionary.namespace}, actual: ${message.namespace}"
        }
        val structure = requireNotNull(dictionary.messages[message.name]) {
            "Unsupported '${message.name}' message type"
        }
        val map = buildMap<String, Any?> {
            structure.fields.keys.forEach { field ->
                put(field, message.getField(field))
            }
            put(MESSAGE_TYPE_FIELD, message.name)
        }
        return MAPPER.writeValueAsString(map).toByteArray()
    }
}

@Suppress("unused")
internal class SimpleSettings : ICommonSettings {
    @DictionaryProperty(MAIN)
    var dictionary: SailfishURI = SailfishURI.unsafeParse("any")
    override fun load(config: HierarchicalConfiguration) { }
}

@Suppress("unused")
internal class Settings : ICommonSettings {
    var fieldBoolean: Boolean = true
    var fieldByte: Byte = 0
    var fieldShort: Short = 0
    var fieldInteger: Int = 0
    var fieldLong: Long = 0
    var fieldFloat: Float = 0.0F
    var fieldDouble: Double = 0.0
    var fieldString: String = ""

    @DictionaryProperty(MAIN)
    var filedMainDictionary: SailfishURI = SailfishURI.unsafeParse("any")

    @DictionaryProperty(LEVEL1)
    var filedLevel1Dictionary: SailfishURI = SailfishURI.unsafeParse("any")
    override fun load(config: HierarchicalConfiguration?) { }
}