/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec.sailfish

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.sailfish.proto.ProtoDecoder
import com.exactpro.th2.codec.sailfish.proto.ProtoEncoder
import com.exactpro.th2.codec.sailfish.transport.TransportDecoder
import com.exactpro.th2.codec.sailfish.transport.TransportEncoder
import com.exactpro.th2.common.grpc.MessageGroup as ProtobufMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup as TransportMessageGroup

class SailfishCodec(
    private val protobufDecoder: ProtoDecoder,
    private val protobufEncoder: ProtoEncoder,
    private val transportDecoder: TransportDecoder,
    private val transportEncoder: TransportEncoder,
) : IPipelineCodec {

    override fun encode(messageGroup: TransportMessageGroup, context: IReportingContext): TransportMessageGroup =
        transportEncoder.process(messageGroup, context)
    override fun decode(messageGroup: TransportMessageGroup, context: IReportingContext): TransportMessageGroup =
        transportDecoder.process(messageGroup, context)
    override fun encode(messageGroup: ProtobufMessageGroup, context: IReportingContext): ProtobufMessageGroup =
        protobufEncoder.process(messageGroup, context)
    override fun decode(messageGroup: ProtobufMessageGroup, context: IReportingContext): ProtobufMessageGroup =
        protobufDecoder.process(messageGroup, context)
}