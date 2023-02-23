# How it works (3.14.2)

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

## Bootstrap parameters

These parameters specify the codec that will be used for the messages decoding/encoding and the mode which should be used.
They should be defined in the `custom-config` section of the component configuration.

```yaml
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

You can set those parameters in `custom-config` as well. You can use the `codecParameters` section in that config.

Example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  custom-config:
    codecClassName: fully.qualified.class.name.for.Factory
    decodeProcessorType: CUMULATIVE
    converterParameters:
      allowUnknownEnumValues: false
      stripTrailingZeros: false
    codecParameters:
      param1: value1
      param2: value2
```

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

### Configuration example
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  custom-config:
    codecClassName: fully.qualified.class.name.for.Factory
    decodeProcessorType: CUMULATIVE
    converterParameters:
      allowUnknownEnumValues: false
      stripTrailingZeros: false
  pins:
    # encoder
    - name: in_codec_encode
      connection-type: mq
      attributes: ['encoder_in', 'parsed', 'subscribe']
    - name: out_codec_encode
      connection-type: mq
      attributes: ['encoder_out', 'raw', 'publish']
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes: ['decoder_in', 'raw', 'subscribe']
    - name: out_codec_decode
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish']
    # encoder general (technical)
    - name: in_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_in', 'parsed', 'subscribe']
    - name: out_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_out', 'raw', 'publish']
    # decoder general (technical)
    - name: in_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_in', 'raw', 'subscribe']
    - name: out_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_out', 'parsed', 'publish']
```

## Message routing

Schema API allows the configuration of routing streams messages via links between connections and filters on pins.
Let's consider some examples of routing in codec box.

### Split on 'publish' pins

For example, you got a big source data stream, and you want to split them into some pins via session alias.
You can declare multiple pins with the attributes `['decoder_out', 'parsed', 'publish']` and filters, instead of a common pin or in addition to it.
Every decoded message will be directed to every declared pins , which will be sent to MQ only if it will pass the filter.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  pins:
    # decoder
    - name: out_codec_decode_first_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish', 'first_session_alias']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: first_session_alias
              operation: EQUAL
    - name: out_codec_decode_secon_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish', 'second_session_alias']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: second_session_alias
              operation: EQUAL
```

The filtering can also be applied for pins with  `subscribe` attribute.

## Release notes

+ 3.14.2
  + Excluded `junit` from dependencies list

+ 3.14.1
    + bom version updated to 4.2.0
    + common updated to 3.44.1

+ 3.14.0
    + Sailfish update to 3.3.54
    + bom version updated to 4.1.0
    + common updated to 3.44.0
    + sailfish utils updated to 3.14.0

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
