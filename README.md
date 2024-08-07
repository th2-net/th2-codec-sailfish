# How it works (5.2.0)

The th2 codec sailfish component is responsible for encoding and decoding the messages.
It operates two instances of encoder/decoder pairs, in which one is used for operational purposes and the other is used for general conversion.
It is based on [th2-codec](https://github.com/th2-net/th2-codec).
You can find additional information [here](https://github.com/th2-net/th2-codec/blob/master/README.md)

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

This project includes only one adapter logic between Sailfish and the th2 packed java library.
This [th2-codec-generic](https://github.com/th2-net/th2-codec-generic) project uses this library as a base.

# Running

The codec requires an implementation of the external codec API.
The JAR file with that implementation and all its dependencies need to be placed to the folder `home/codec_implementation`, in order to start the codec.

The codec loads all JAR files from that directory and looks for all the implementations of
[com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt) interface.
After that, it loads the factory defined in the configuration, and it creates the codec using that factory.

# Creating your own codec

You can create a codec for your protocol by implementing the following interface - [com.exactpro.sf.externalapi.codec.IExternalCodec](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodec.kt).
Also, you need to implement the interface [com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt).

The core part of the "Codec" component uses [ServiceLoader](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/ServiceLoader.html) to load all the factory interface implementations.
In order to provide the ServiceLoader with the knowledge about your factory implementation, the JAR file should contain a provider-configuration file named:

**META-INF/services/com.exactpro.sf.externalapi.codec.IExternalCodecFactory**

with the content that is equal to the fully-qualified class name of your factory implementation.

_If you have several implementations regarding that interface, each one of their fully-qualified names should be written in a new line, inside that file._

## Bootstrap parameters

These parameters specify the codec that will be used for the messages decoding/encoding and the mode which should be used.
They should be defined in the `customConfig.codecSettings` section of the component configuration.

```yaml
codecClassName: fully.qualified.class.name.for.Factory
converterParameters:
  allowUnknownEnumValues: false # allows unknown enum values during message encoding
  stripTrailingZeros: false # removes trailing zeroes for `BigDecimal` (_0.100000_ -> _0.1_)
```

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

You can set those parameters in `customConfig.codecSettings` as well. You can use the `codecParameters` section in that config.

Example:
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  customConfig:
    enableVerticalScaling: false
    isFirstCodecInPipeline: true
    disableMessageTypeCheck: false
    disableProtocolCheck: false
    eventPublication:
      flushTimeout: 1000
      batchSize: 100
    codecSettings:
      codecClassName: fully.qualified.class.name.for.Factory
      converterParameters:
        allowUnknownEnumValues: false
        stripTrailingZeros: false
      codecParameters:
        param1: value1
        param2: value2
```

## Dictionaries definition based on aliases

You can set desired dictionaries using its aliases.
Note that this part of the configuration is optional. If it is not specified, dictionaries will be loaded based on type.
If a dictionary with the required type is not found, an exception will be thrown. A detailed list with found and required dictionaries will also be logged.

Example: 

```yaml
    dictionaries:
      MAIN: "${dictionary_link:dictionary-cr-name-A}"
      LEVEL1: "${dictionary_link:dictionary-cr-name-B}"
```

### Configuration example
```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  customConfig:
    enableVerticalScaling: false
    isFirstCodecInPipeline: true
    disableMessageTypeCheck: false
    disableProtocolCheck: false
    eventPublication:
      flushTimeout: 1000
      batchSize: 100
    codecSettings:
      codecClassName: fully.qualified.class.name.for.Factory
      converterParameters:
        allowUnknownEnumValues: false
        stripTrailingZeros: false
      dictionaries:
        MAIN: "${dictionary_link:dictionary-cr-name-A}"
        LEVEL1: "${dictionary_link:dictionary-cr-name-B}"
    transportLines:
      transport:
        type: TH2_TRANSPORT
        useParentEventId: true
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
You can declare multiple pins with the attributes `['<prefix>_decoder_out', 'transport-group', 'publish']` and filters, instead of a common pin or in addition to it.
Every decoded message will be directed to every declared pins , which will be sent to MQ only if it will pass the filter.

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: codec
spec:
  customConfig:
    transportLines:
      transport:
        type: TH2_TRANSPORT
        useParentEventId: true
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
        - name: out_codec_transport_encode
          attributes:
            - transport_encoder_out
            - transport-group
            - publish
```

The filtering can also be applied for pins with  `subscribe` attribute.

## Release notes

### 5.2.0
+ Updated:
  + th2-gradle-plugin `0.1.1`
  + common: `5.14.0-dev`
  + sailfish: `3.3.241`

### 5.1.0
+ Migrated to th2 gradle plugin: `0.0.6`
+ Updated:
  + bom `4.6.1`
  + common: `5.10.1-dev`
  + common-utils: `2.2.3-dev`
  + codec: `5.5.0-dev`
  + sailfish: `3.3.202`

+ 5.0.0
  + Migrate to th2-codec base
  + sailfish: `3.3.169`
  + common: `5.7.2-dev`

+ 4.2.3
  + Fix empty prefix in the pin attributes
  + Fix missing protocol in message during decoding
  * common: `5.5.0-dev`
  * common-utils: `2.2.2-dev` (fixed batching bug when max batch size == 1)
  * sailfish-utils: `4.1.1-dev`

+ 4.2.2
  + use 'codecClassName' parameter to identify the codec implementation in case more than one is found in classpath

+ 4.2.1
  * added task-utils: `0.1.2`

+ 4.2.0
  * bom: `4.5.0-dev`
  * common: `5.4.0-dev`
  * common-utils: `2.2.0-dev`
  * sailfish-utils: `4.1.0-dev`
  * kotlin: `1.8.22`

+ 4.1.1
  * Added transport lines to declare serveral independnet encode/decode group

+ 4.1.0
  * Transport protocol support
  * Updated common: `5.3.2-dev`
  * Updated common-utils: `2.1.1-dev`
  * Updated sailfish-utils: `4.0.1-dev`
  
+ 4.1.0
   * Transport protocol support
  
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