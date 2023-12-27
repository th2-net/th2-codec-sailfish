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

package com.exactpro.th2.codec.sailfish.configuration

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
import com.exactpro.th2.codec.api.impl.PipelineCodecContext
import com.exactpro.th2.codec.sailfish.SailfishCodecFactory
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.google.auto.service.AutoService
import org.apache.commons.configuration.HierarchicalConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.eq
import org.mockito.kotlin.mock

class TestConfiguration {
    private val mainDictionary = """<dictionary xmlns="http://exactprosystems.com/dictionary" name="MAIN"></dictionary>"""
    private val level1Dictionary = """<dictionary xmlns="http://exactprosystems.com/dictionary" name="LEVEL1"></dictionary>"""

    private val mainDictionaryStructure = mainDictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)
    private val level1DictionaryStructure = level1Dictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)

    private val commonFactory: CommonFactory = mock {
        on { loadDictionary(eq(DictionaryType.MAIN.name)) }.thenReturn(mainDictionary.byteInputStream())
        on { loadDictionary(eq(DictionaryType.LEVEL1.name)) }.thenReturn(level1Dictionary.byteInputStream())
    }

    @Test
    fun `codec configuration test`() {
        val codecSettings = SailfishConfiguration().apply {
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

        val settings: IExternalCodecSettings = SailfishCodecFactory().apply {
            init(PipelineCodecContext(commonFactory))
            create(codecSettings)
        }.codecSettings

        assertEquals(mainDictionaryStructure.namespace, settings[MAIN]?.namespace)
        assertEquals(level1DictionaryStructure.namespace, settings[LEVEL1]?.namespace)

        assertEquals(true, settings["fieldBoolean"])
        assertEquals(10, settings["fieldByte"])
        assertEquals(20, settings["fieldShort"])
        assertEquals(0, settings["fieldInteger"])
        assertEquals(20, settings["fieldLong"])
        assertEquals(0.0F, settings["fieldFloat"])
        assertEquals(10.10, settings["fieldDouble"])
        // main dictionary type is replaced because Sailfish codec external API gets main dictionary by type
        assertEquals(SailfishURI.unsafeParse("any"), settings["filedMainDictionary"])
        assertEquals(LEVEL1.toUri(), settings["filedLevel1Dictionary"])
    }
}

@AutoService(IExternalCodecFactory::class)
internal class CodecFactory : IExternalCodecFactory {
    override val protocolName: String = "test"

    override fun createCodec(settings: IExternalCodecSettings): IExternalCodec = Codec()
    override fun createSettings(): IExternalCodecSettings = ExternalCodecSettings(Settings())
    @Deprecated("Set dictionary on an instance instead", replaceWith = ReplaceWith("createSettings()"))
    override fun createSettings(dictionary: IDictionaryStructure): IExternalCodecSettings =
        throw UnsupportedOperationException("'createSettings' method is unsupported")
}

internal class Codec : IExternalCodec {
    override fun close() { }
    override fun decode(data: ByteArray): List<IMessage> {
        throw UnsupportedOperationException("'decode' method is unsupported")
    }
    override fun encode(message: IMessage): ByteArray {
        throw UnsupportedOperationException("'encode' method is unsupported")
    }
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
    override fun load(config: HierarchicalConfiguration?) {
        TODO("Not yet implemented")
    }
}