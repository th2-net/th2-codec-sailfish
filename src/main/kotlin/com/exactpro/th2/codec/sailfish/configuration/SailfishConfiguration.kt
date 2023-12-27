/*
 *  Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.sailfish.configuration

import com.exactpro.th2.codec.api.IPipelineCodecSettings

class SailfishConfiguration(
    val defaultSettingResourceName: String = "codec_config.yml",
    val converterParameters: ConverterParameters = ConverterParameters(),
    var dictionaries: Map<String, String> = emptyMap(),
): IPipelineCodecSettings {
    var codecClassName: String? = null

    var codecParameters: Map<String, String> = emptyMap()
}

class ConverterParameters(
    val stripTrailingZeros: Boolean = false,
    val allowUnknownEnumValues: Boolean = false
)



