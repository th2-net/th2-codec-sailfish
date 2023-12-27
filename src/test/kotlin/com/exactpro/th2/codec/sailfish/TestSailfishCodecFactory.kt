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
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.api.impl.PipelineCodecContext
import com.exactpro.th2.codec.sailfish.configuration.SailfishConfiguration
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.google.auto.service.AutoService
import com.google.protobuf.UnsafeByteOperations
import org.apache.commons.configuration.HierarchicalConfiguration
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.eq
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
private const val FIELD = "test-field"

class TestSailfishCodecFactory {
    private val mainDictionary =
        """<dictionary xmlns="http://exactprosystems.com/dictionary" name="MAIN">
             <messages>
               <message name="$MESSAGE_TYPE">
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
            DictionaryType.LEVEL1.name to DictionaryType.LEVEL1.name,
        )
        codecParameters = mapOf(
            "fieldBoolean" to "true",
            "fieldShort" to "20",
            "fieldLong" to "20",
            "fieldString" to "test",
        )
    }

    private val codec: IPipelineCodec = SailfishCodecFactory().run {
        init(PipelineCodecContext(commonFactory))
        create(this@TestSailfishCodecFactory.codecSettings)
    }

    private val context = mock<IReportingContext> { }

    @AfterEach
    fun afterEach() {
        verifyNoInteractions(context)
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
                            put("fieldBoolean", true)
                            put("fieldByte", 10.toByte())
                            put("fieldShort", 20.toShort())
                            put("fieldInteger", 0)
                            put("fieldLong", 20L)
                            put("fieldFloat", 0.0F)
                            put("fieldDouble", 10.10)
                            put("fieldString", "test")
                            // main dictionary type is replaced because Sailfish codec external API gets main dictionary by type
                            put("filedMainDictionary", SailfishURI.unsafeParse("any"))
                            put("filedLevel1Dictionary", LEVEL1.toUri())
                            put(MAIN.name, mainDictionaryStructure.namespace)
                            put(LEVEL1.name, level1DictionaryStructure.namespace)
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
                            setBody(byteArrayOf(1, 2, 3))
                        }.build()
                    ).build(), context
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
                        addField("fieldBoolean", true)
                        addField("fieldByte", 10.toByte())
                        addField("fieldShort", 20.toShort())
                        addField("fieldInteger", 0)
                        addField("fieldLong", 20L)
                        addField("fieldFloat", 0.0F)
                        addField("fieldDouble", 10.10)
                        addField("fieldString", "test")
                        // main dictionary type is replaced because Sailfish codec external API gets main dictionary by type
                        addField("filedMainDictionary", SailfishURI.unsafeParse("any"))
                        addField("filedLevel1Dictionary", LEVEL1.toUri())
                        addField(MAIN.name, mainDictionaryStructure.namespace)
                        addField(LEVEL1.name, level1DictionaryStructure.namespace)
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
                            setBody(UnsafeByteOperations.unsafeWrap(byteArrayOf(1, 2, 3)))
                        }
                }.build(), context
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
                        setBody("test-message-type : test-field=test-value|rawMessage=".toByteArray())
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
                    ).build(), context
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
                        setBody(UnsafeByteOperations.unsafeWrap("test-message-type : test-field=test-value|rawMessage=".toByteArray()))
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
                }.build(), context
            )
        )
    }
}

@AutoService(IExternalCodecFactory::class)
internal class CodecFactory : IExternalCodecFactory {
    override val protocolName: String = PROTOCOL

    override fun createCodec(settings: IExternalCodecSettings): IExternalCodec = Codec(settings)
    override fun createSettings(): IExternalCodecSettings = ExternalCodecSettings(Settings())

    @Deprecated("Set dictionary on an instance instead", replaceWith = ReplaceWith("createSettings()"))
    override fun createSettings(dictionary: IDictionaryStructure): IExternalCodecSettings =
        throw UnsupportedOperationException("'createSettings' method is unsupported")
}

internal class Codec(
    private val settings: IExternalCodecSettings
) : IExternalCodec {
    override fun close() {}
    override fun decode(data: ByteArray): List<IMessage> = listOf(
        getFactory().createMessage(MESSAGE_TYPE, settings[MAIN]?.namespace).apply {
            metaData.rawMessage = data
            settings.propertyTypes.keys.forEach {
                addField(it, settings[it])
            }
            settings.dictionaryTypes.forEach {
                addField(it.name, settings[it]?.namespace)
            }
        }
    )

    override fun encode(message: IMessage): ByteArray = message.toString().toByteArray(Charsets.UTF_8)
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