# How it works (4.1.1)

The th2 Codec component is responsible for encoding and decoding the messages.
It operates two instances of encoder/decoder pairs, in which one is used for operational purposes and the other is used for general conversion.

Encoding and decoding are performed according to the scheme "one or more input pins and one or more output pins".
Both types of pins may have filters. The input / output of the encoder and decoder can be partially or entirely filtered out depending on which filters the pin has.
The metadata of the message and its fields can be used as filter parameters.

One instance of the codec implements the logic for encoding and decoding one protocol of one version.
The version-specific protocol messages are described in a separate XML file called "dictionary".
Codec operates with arrays of messages (parsed batch to raw batch in case of encoding and raw batch to parsed batch upon decoding).

## Encode 
During encoding codec must replace each parsed message of supported or unknown protocol in a message group with a raw one by encoding parsed message's content.

## Decode
During decoding codec must replace each raw message of supported or unknown protocol in a message group with one or several parsed by decoding raw message's body.

## Appointment

This project includes only one adapter logic between Sailfish and the th2 packed into the Docker Image.
This [th2-codec-generic](https://github.com/th2-net/th2-codec-generic) project uses this image as a base.

# Running

The codec requires an implementation of the external codec API.
The JAR file with that implementation and all its dependencies need to be placed to the folder `home/codec_implementation`, in order to start the codec.

The codec loads all JAR files from that directory and looks for all the implementations of
[com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt) interface.
After that, it loads the factory defined in the configuration and it creates the codec using that factory.

# Creating your own codec

You can create a codec for your protocol by implementing the following interface - [com.exactpro.sf.externalapi.codec.IExternalCodec](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodec.kt).
Also, you need to implement the interface [com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt).

The core part of the "Codec" component uses [ServiceLoader](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/ServiceLoader.html) to load all the factory interface implementations.
In order to provide the ServiceLoader with the knowledge about your factory implementation, the JAR file should contain a provider-configuration file named:

**META-INF/services/com.exactpro.sf.externalapi.codec.IExternalCodecFactory**

with the content that is equal to the fully-qualified class name of your factory implementation.

_If you have several implementations regarding that interface, each one of their fully-qualified names should be written in a new line, inside that file._


# Configuration

Codec has four types of connections: stream and general for encode and decode functions.

* stream encode / decode connections works 24 / 7
* general encode / decode connections works on demand

Codec never mixes messages from the _stream_ and the _general_ connections. 

Decoding can work in two different modes:
+ **CUMULATIVE** (default) - all raw messages in the batch will be joined together and decoded. After decoding them, the content and the count of the decoded messages will be compared to the original messages in the batch.
+ **SEQUENTIAL** - each message in the batch will be decoded as a separate message.

This setting can be overridden in a custom config for the application using the parameter `decodeProcessorType`.

## Transport lines

transportLines responsable for number of independnet ecoding / decoding lines. Each transport lines has type option
* type is enum [`PROTOBUF`, `TH2_TRANSPORT`] value. Codec creates suitable type of message processor accoding this option.
  NOTE: Support of each transport depends on childe codec implementation.

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
   name: codec
spec:
   customConfig:
      transportLines:
        "": PROTOBUF
        general: PROTOBUF
        transport: TH2_TRANSPORT
        general_transport: TH2_TRANSPORT
   pins:
     mq:
        subscribers:
#          prefix "" 
           - name: in_codec_decode
             attributes:
                - decoder_in
                - subscribe
           - name: in_codec_encode
             attributes:
                - encoder_in
                - subscribe
#          prefix "general" 
           - name: in_codec_general_decode
             attributes:
                - general_decoder_in
                - subscribe
           - name: in_codec_general_encode
             attributes:
                - general_encoder_in
                - subscribe
#          prefix "transport" 
           - name: in_codec_transport_decode
             attributes:
                - transport_decoder_in
                - transport-group
                - subscribe
           - name: in_codec_transport_encode
             attributes:
                - transport_encoder_in
                - transport-group
                - subscribe
#          prefix "general_transport" 
           - name: in_codec_general_transport_decode
             attributes:
                - general_transport_decoder_in
                - transport-group
                - subscribe
           - name: in_codec_general_transport_encode
             attributes:
                - general_transport_encoder_in
                - transport-group
                - subscribe
        publishers:
#          prefix ""
           - name: out_codec_decode
             attributes:
                - decoder_out
                - publish
           - name: out_codec_encode
             attributes:
                - encoder_out
                - publish
#          prefix "general"
           - name: out_codec_general_decode
             attributes:
                - general_decoder_out
                - publish
           - name: out_codec_general_encode
             attributes:
                - general_encoder_out
                - publish
#          prefix "transport"
           - name: out_codec_transport_decode
             attributes:
                - transport_decoder_out
                - transport-group
                - publish
           - name: out_codec_transport_encode
             attributes:
                - transport_encoder_out
                - transport-group
                - publish
#          prefix "general_transport"
           - name: out_codec_general_transport_decode
             attributes:
                - general_transport_decoder_out
                - transport-group
                - publish
           - name: out_codec_general_transport_encode
             attributes:
                - general_transport_encoder_out
                - transport-group
                - publish
```

## Bootstrap parameters

These parameters specify the codec that will be used for the messages decoding/encoding and the mode which should be used.
They should be defined in the `customConfig` section of the component configuration.

```yaml
enabledExternalQueueRouting: false #option to enable/disable external queue routing logic. Default value is false.
enableVerticalScaling: false #option to control vertical scaling mode. Codec splits an incoming batch into message groups and process each of them via the CompletableFuture. The default value is `false`. Please note this is experimental feature.
codecClassName: fully.qualified.class.name.for.Factory
decodeProcessorType: CUMULATIVE
converterParameters:
  allowUnknownEnumValues: false # allows unknown enum values during message encoding
  stripTrailingZeros: false # removes trailing zeroes for `BigDecimal` (_0.100000_ -> _0.1_)
```

## Publishing events parameters

These parameters determine the size of the EventBatch, and the time (seconds) during which the EventBatch is built.

```yaml
outgoingEventBatchBuildTime: 30
maxOutgoingEventBatchSize: 99
numOfEventBatchCollectorWorkers: 1
```

**outgoingEventBatchBuildTime** - time interval in seconds to publish the collected events reported by the codec
**maxOutgoingEventBatchSize** - the max number of events in a single batch.
If events count exceeds that amount the batch will be published earlier than the `outgoingEventBatchBuildTime`.
**numOfEventBatchCollectorWorkers** - the number of threads to process published events.
Higher number means that there might be more batches published concurrently.
But increasing that number might affect performance if number of available cores is less than this number.

## Codec implementation parameters

These parameters will be passed to the actual codec implementation to configure its behavior.
It's possible that a codec might not require to configure any parameters. In this case, you can ommit adding those parameters.

The codec implementation parameters should be located in the container's `/home` directory and stored into the file named `codec_config.yml`.
It has a simple key-value format in YAML.
```yaml
---
param1: value1
param2: value2
```
The set of parameters depends on the codec implementation that is used.

The parameters from that file are static and will be loaded during the codec start up. You can use them to provide the defaults for some implementations' parameters.

You can set those parameters in `customConfig` as well. You can use the `codecParameters` section in that config.

Example:
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  customConfig:
    codecClassName: fully.qualified.class.name.for.Factory
    decodeProcessorType: CUMULATIVE
    converterParameters:
      allowUnknownEnumValues: false
      stripTrailingZeros: false
    codecParameters:
      param1: value1
      param2: value2
```

## Codec transport lines parameter
This parameter gives user ability to define how many in/out pin will be there and their types ( PROTOBUF/TRANSPORT )
Transport pins are used to decode/encode messages to/from Transport messages.
Protobuf pins are used to decode/encode messages to/from Protobuf messages.

This parameter is a map. Key is prefix to pin name. Value is the type of pin.

Example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  custom-config:
    transportLines:
      - simple: PROTOBUF
      - general: PROTOBUF
```

This configuration tells codec to create two pairs of encoder&decoder and use `simple_decoder_in`, `simple_decoder_out`, `general_decoder_in`, `general_decoder_out` pins for these codecs from mq configuration section.  

## Required pins

Every type of connection has two `subscribe` and `publish` pins.
The first one is used to receive messages to decode/encode while the second one is used to send decoded/encoded messages further.
**Configuration should include at least one pin for each of the following sets of attributes:**
+ Pin for the stream encoding input: `encoder_in` `parsed` `subscribe`
+ Pin for the stream encoding output: `encoder_out` `raw` `publish`
+ Pin for the general encoding input: `general_encoder_in` `parsed` `subscribe`
+ Pin for the general encoding output: `general_encoder_out` `raw` `publish`
+ Pin for the stream decoding input: `decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `decoder_out` `parsed` `publish`
+ Pin for the stream decoding input: `general_decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `general_decoder_out` `parsed` `publish`

+ Pin for the stream encoding input: `transport_encoder_in` `transport-group` `subscribe`
+ Pin for the stream encoding output: `transport_encoder_out` `transport-group` `publish`
+ Pin for the general encoding output: `transport_general_encoder_out` `transport-group` `publish`
+ Pin for the general encoding input: `transport_general_encoder_in` `transport-group` `subscribe`
+ Pin for the stream decoding input: `transport_decoder_in` `transport-group` `subscribe`
+ Pin for the stream decoding output: `transport_decoder_out` `transport-group` `publish`
+ Pin for the stream decoding input: `transport_general_decoder_in` `transport-group` `subscribe`
+ Pin for the stream decoding output: `transport_general_decoder_out` `transport-group` `publish`

## Dictionaries definition based on aliases

You can set desired dictionaries using its aliases.
Note that this part of the configuration is optional. If it is not specified, dictionaries will be loaded based on type.
If a dictionary with the required type is not found, an exception will be thrown. A detailed list with found and required dictionaries will also be logged.

Example: 

```yaml
    dictionaries:
      MAIN: aliasA
      LEVEL1: aliasB
```

### Configuration example
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  customConfig:
    enabledExternalQueueRouting: false
    enableVerticalScaling: false
    codecClassName: fully.qualified.class.name.for.Factory
    decodeProcessorType: CUMULATIVE
    converterParameters:
      allowUnknownEnumValues: false
      stripTrailingZeros: false
    dictionaries:
      MAIN: aliasA
      LEVEL1: aliasB
    transportLines:
      transport: TH2_TRANSPORT
  pins:
    mq:
      subscribers:
        - name: in_codec_transport_decode
          attributes:
            - transport_decoder_in
            - transport-group
            - subscribe
        - name: in_codec_transport_encode
          attributes:
            - transport_encoder_in
            - transport-group
            - subscribe
      publishers:
        - name: out_codec_transport_decode
          attributes:
            - transport_decoder_out
            - transport-group
            - publish
        - name: out_codec_transport_encode
          attributes:
            - transport_encoder_out
            - transport-group
            - publish
```

## Message routing

Schema API allows the configuration of routing streams messages via links between connections and filters on pins.
Let's consider some examples of routing in codec box.

### Split on 'publish' pins

For example, you got a big source data stream, and you want to split them into some pins via session alias.
You can declare multiple pins with the attributes `['decoder_out', 'parsed', 'publish']` and filters, instead of a common pin or in addition to it.
Every decoded message will be directed to every declared pins , which will be sent to MQ only if it will pass the filter.

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  pins:
    mq:
      publishers:
        # decoder
        - name: out_codec_transport_decode_first_session_alias
          attributes:
            - transport_decoder_out
            - transport-group
            - publish
          filters:
            - metadata:
                - expectedValue: "first_session_alias_*"
                  fieldName: session_alias
                  operation: WILDCARD
        - name: out_codec_transport_decode_second_session_alias
          attributes:
            - transport_decoder_out
            - transport-group
            - publish
          filters:
            - metadata:
                - expectedValue: "second_session_alias_*"
                  fieldName: session_alias
                  operation: WILDCARD
```

The filtering can also be applied for pins with  `subscribe` attribute.

## Release notes

+ 4.1.1
  * Added transport lines to declare serveral independnet encode/decode group

+ 4.1.0
  * Transport protocol support
  * Updated common: `5.3.2-dev`
  * Updated common-utils: `2.1.1-dev`
  * Updated sailfish-utils: `4.0.1-dev`
  
+ 4.0.2
  * Fixed: excluded vulnerable dependencies
  * Migration to common: 5.2.0-dev

+ 4.0.1
  * Fixed: codec does not publish error events

+ 4.0.0
  * Migration to kotlin:1.6.21
  * Migration to sailfish:3.3.54
  * Migration to bom:4.2.0
  * Migration to common:5.1.0
  + Migration to books & pages concept
  + Added ability to define dictionaries via custom config in based on sailfish adapters
  + Uses event batcher which supports publication by timeout and packs events into a batch by specified count and size in bytes.
  + Added ability to redirect messages using externalQueue attribute
  + Migrated to log4j2
  + Added option to disable vertical scaling

+ 3.14.1
  + message batch will be processed asynchronously if more than one CPU core is available

+ 3.14.4
  + Included `apache-mina-core` to dependencies list

+ 3.14.3
  + Excluded `apache-mina-core` from dependencies list

+ 3.14.2
  + Excluded `junit` from dependencies list

+ 3.14.1
  + bom version updated to 4.2.0
  + common updated to 3.44.1

+ 3.14.0
  + Dependencies with vulnerabilities was updated
  + The common library update from 3.32.0 to 3.44.0
    + Filter behavior is corrected: only messages that does not match filter are dropped instead of the whole group
    + Log4j2 is used. Requires logging configuration updates
  + The sailfish-utils library update from 3.12.3 to 3.13.0
    + Changed the format for time and date time (always includes milliseconds part)
  + The sailfish-core library update to 3.14.0
  + Deprecated `registerModule(KotlinModule())` was replaced with `registerKotlinModule()`

+ 3.13.0
    + Codec handles messages with its protocol or empty during encode/decode
    + The common library update from 3.29.2 to 3.32.0 
    + The sailfish-utils library update from 3.8.0 to 3.12.3 
    + The sailfish-core library update from 3.2.1741 to 3.2.1776 
    + The kotlin update from 1.3.71 to 1.5.30 
    + The kotlin-logging library update from 1.7.+ to 2.0.11 

+ 3.12.3
    + Update sailfish dependencies from `3.2.1674` to `3.2.1741`
    + Change default value for `outgoingEventBatchBuildTime`.
      The value defines time in seconds the previous default value caused a long delay before event reporting
    + Replaced custom protobuf message printing with `MessageUtils.toJson()`
    + Use name from the schema for codec's root event
    + Add information about codec's parameters into a body for root event
    + The common library update from 3.25.1 to 3.29.2
      + Fix filtering by `message_type` for pins

+ 3.12.2
    + Fix error when we try to synchronize on `lateinit` property when it is not initialized yet

+ 3.12.1
    + Publish error event when no message was produced during decoding and the th2-error-message is published instead.

+ 3.12.0
    + Update `sailfish-core` version to `3.2.1674`

+ 3.11.0
    + Update sailfish-utils to 3.8.0
    + Add **stripTrailingZeros** parameter for removing trailing zeroes for `BigDecimal` values during the decoding
    + Move converter parameters to a separate object

+ 3.10.1
    + Update sailfish-core version to 3.2.1655 (fix problem with properties in settings that have setters created using builder pattern)

+ 3.10.0
    + Update common version to 3.17.0
    + Update sailfish-utils to 3.6.0
    + Add parameter to allow unknown enum values during encoding

+ 3.9.1
    + Add a notification about ErrorMessage during decoding and any codec errors via failed event

+ 3.9.0
    + Disable waiting for connection recovery when closing the `SubscribeMonitor`
    
+ 3.8.1
    + Updated codec actions on error, new message with th2-codec-error type will be generated - message contains information about the problem and raw message inside
  
+ 3.8.0
    + Set message protocol of encoded/decoded messages according to used codec

+ 3.7.2
    + Added extraction of messages from EvolutionBatch when decoding RawMessages. This is necessary if evolutionSupportEnabled mode is set to true - Sailfish codecs package the decoding results in EvolutionBatch.

+ 3.7.1
    + Updated sailfish-utils to 3.3.4 - optimized converter Value.SIMPLE_VALUE to Java Class
    + Updated sailfish-core to 3.2.1583 - removed method call MessageWrapper.cloneMessage to improve performance

+ 3.7.0
    + Update common library to 3.14.0
    + Copy properties from th2 proto Message to the Sailfish IMessage when converting
    + Use release version for sailfish-core

+ 3.6.2
    + fixed dictionary reading

+ 3.6.1
    + removed gRPC event loop handling
    + fixed dictionary reading

+ 3.6.0
    + reads dictionaries from the /var/th2/config/dictionary folder.
    + uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
    + tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration

+ 3.5.1
    + Update sailfish-utils to fix a problem with message names

+ 3.5.0
    + Checks message structure by the configured dictionary during encode.

+ 3.4.0
    + Validates configured dictionaries during initialization

+ 3.3.2
    + Allow the codec produce more than one message during decoding

+ 3.3.1
    + Updated core version. Introduce the embedded pipeline for Netty

+ 3.3.0
    + Copies a parent event id from the raw to the parsed message
