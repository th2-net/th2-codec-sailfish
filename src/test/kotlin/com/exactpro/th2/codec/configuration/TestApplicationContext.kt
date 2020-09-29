/******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.codec.configuration

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.messages.structures.IDictionaryStructure
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.configuration.workspace.FolderType
import com.exactpro.sf.externalapi.codec.*
import com.exactpro.th2.schema.dictionary.DictionaryType.LEVEL1
import com.exactpro.th2.schema.dictionary.DictionaryType.MAIN
import com.exactpro.th2.schema.factory.CommonFactory
import com.exactpro.th2.schema.grpc.configuration.GrpcRouterConfiguration
import com.exactpro.th2.schema.grpc.router.GrpcRouter
import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table
import io.grpc.BindableService
import io.grpc.Server
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.junit.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mockStatic
import java.io.File
import java.util.*
import javax.xml.parsers.SAXParserFactory
import javax.xml.validation.SchemaFactory
import com.exactpro.sf.externalapi.DictionaryType as SailfishDictionaryType

class TestApplicationContext {
    @Test
    fun testDictionarySetting() {
        val configuration = Configuration().apply { codecClassName = CodecFactory::class.java.name }
        val commonFactory = Mockito.mock(CommonFactory::class.java)

        `when`(commonFactory.grpcRouter).thenReturn(object : GrpcRouter {
            override fun close(): Unit = TODO("Not yet implemented")
            override fun init(p0: GrpcRouterConfiguration?): Unit = TODO("Not yet implemented")
            override fun <T : Any?> getService(p0: Class<T>): T? = null
            override fun startServer(vararg p0: BindableService?): Server = TODO("Not yet implemented")
        })

        val mainDictionary = """<dictionary xmlns="http://exactprosystems.com/dictionary" name="MAIN"></dictionary>"""
        val level1Dictionary = """<dictionary xmlns="http://exactprosystems.com/dictionary" name="LEVEL1"></dictionary>"""

        val mainDictionaryStructure = mainDictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)
        val level1DictionaryStructure = level1Dictionary.byteInputStream().use(XmlDictionaryStructureLoader()::load)

        `when`(commonFactory.readDictionary(eq(MAIN))).thenReturn(mainDictionary.byteInputStream())
        `when`(commonFactory.readDictionary(eq(LEVEL1))).thenReturn(level1Dictionary.byteInputStream())

        val codecLoader = Mockito.mock(ServiceLoader::class.java).apply {
            `when`(iterator()).thenReturn(arrayListOf(CodecFactory()).iterator())
        }

        val emptyLoader = Mockito.mock(ServiceLoader::class.java).apply {
            `when`(iterator()).thenReturn(arrayListOf<Any>().iterator())
        }

        mockStatic(FileUtils::class.java).use { utilsMock ->
            utilsMock.`when`<Any> { FileUtils.listFiles(any(File::class.java), any(Array<String>::class.java), anyBoolean()) }.thenReturn(listOf<Any>())

            mockStatic(ServiceLoader::class.java).use { loaderMock ->
                loaderMock.apply {
                    `when`<Any> { ServiceLoader.load(any(Class::class.java), any(ClassLoader::class.java)) }.thenReturn(codecLoader)
                    `when`<Any> { ServiceLoader.load(eq(SchemaFactory::class.java)) }.thenReturn(emptyLoader)
                    `when`<Any> { ServiceLoader.load(eq(SAXParserFactory::class.java)) }.thenReturn(emptyLoader)
                }

                val settings = ApplicationContext.create(configuration, commonFactory).codecSettings

                Assert.assertEquals(mainDictionaryStructure.namespace, settings[SailfishDictionaryType.MAIN]!!.namespace)
                Assert.assertEquals(level1DictionaryStructure.namespace, settings[SailfishDictionaryType.LEVEL1]!!.namespace)
            }
        }
    }

    private class CodecFactory : IExternalCodecFactory {
        override val protocolName: String = "test"

        override fun createCodec(settings: IExternalCodecSettings): IExternalCodec = Codec()
        override fun createSettings(): IExternalCodecSettings = Settings()
        override fun createSettings(dictionary: IDictionaryStructure): IExternalCodecSettings = TODO("Not yet implemented")

        private class Codec : IExternalCodec {
            override fun close(): Unit = TODO("Not yet implemented")
            override fun decode(data: ByteArray): List<IMessage> = TODO("Not yet implemented")
            override fun encode(message: IMessage): ByteArray = TODO("Not yet implemented")
        }

        private class Settings : IExternalCodecSettings {
            private val dictionaries = hashMapOf<SailfishDictionaryType, IDictionaryStructure>()

            override val dataFiles: MutableMap<SailfishURI, File> = hashMapOf()
            override val dataResources: Table<PluginAlias, ResourcePath, File> = HashBasedTable.create()
            override val dictionaryFiles: MutableMap<SailfishURI, File> = hashMapOf()
            override val dictionaryTypes: Set<SailfishDictionaryType> = setOf(SailfishDictionaryType.MAIN, SailfishDictionaryType.LEVEL1)
            override val propertyTypes: Map<String, Class<*>> = mapOf()
            override val workspaceFolders: MutableMap<FolderType, File> = EnumMap(FolderType::class.java)
            override fun get(dictionaryType: SailfishDictionaryType): IDictionaryStructure? = dictionaries[dictionaryType]
            override fun <T> get(propertyName: String): T = TODO("Not yet implemented")
            override fun <T : Any> getSettings(): T = TODO("Not yet implemented")
            override fun set(dictionaryType: SailfishDictionaryType, dictionary: IDictionaryStructure) = dictionaries.set(dictionaryType, dictionary)
            override fun set(propertyName: String, propertyValue: Any?) = TODO("Not yet implemented")
        }
    }
}