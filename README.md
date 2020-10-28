= How it works

The th2 Codec component is responsible for encoding and decoding the messages. It is operating two instances of encoder/decoder pairs, one for operational purposes and one for general conversion.

Encoding and decoding is performed according to the scheme "one input queue and one or more output queues". Output queues can have filters, depending on which the output of the encoder and decoder can be partially or completely filtered out. The metadata of the message and its fields are used as filter parameters.

One instance of the codec implements the logic for encoding and decoding one protocol of one version. Version-specific protocol messages are described in a separate xml file called a "dictionary".
Codec operates with arrays of messages (parsed batch to raw batch in case of encoding and raw batch to parsed batch upon decoding).

= Running

To start a codec it is required to place external codec api implementation jar with dependencies to 'codec_implementation' folder and dictionaries in xml format to 'dictionaries' folder. These folder must be placed in the working directory of codec  application.

= Configuration
== Message Router
Example of `mq.json` config file:

[source]
----
{
    "queues": {
        "encodeQueueIn":{
            "name": "encodeQueueIn",
            "queue": "encodeQueueIn",
            "exchange": "exchange",
            "attributes": ["subscribe", "parsed", "encode_in"]
        },
        "encodeQueueOut": {
            "name": "encodeQueueOut",
            "queue": "encodeQueueOut",
            "exchange": "exchange",
            "attributes": ["publish", "raw", "encode_out"]
        },
        "decodeQueueIn": {
            "name": "decodeQueueIn",
            "queue": "decodeQueueIn",
            "exchange": "exchange",
            "attributes": ["subscribe", "raw", "decode_in"]
        },
        "decodeQueueOut": {
            "name": "decodeQueueOut",
            "queue": "decodeQueueOut",
            "exchange": "exchange",
            "attributes": ["publish", "parsed", "decode_out"]
        },
        "generalEncodeQueueIn":{
            "name": "generalEncodeQueueIn",
            "queue": "generalEncodeQueueIn",
            "exchange": "exchange",
            "attributes": ["subscribe", "parsed", "general_encode_in"]
        },
        "generalEncodeQueueOut": {
            "name": "generalEncodeQueueOut",
            "queue": "generalEncodeQueueOut",
            "exchange": "exchange",
            "attributes": ["publish", "raw", "general_encode_out"]
        },
        "generalDecodeQueueIn": {
            "name": "generalDecodeQueueIn",
            "queue": "generalDecodeQueueIn",
            "exchange": "exchange",
            "attributes": ["subscribe", "raw", "general_decode_in"]
        },
        "generalDecodeQueueOut": {
            "name": "generalDecodeQueueOut",
            "queue": "generalDecodeQueueOut",
            "exchange": "exchange",
            "attributes": ["publish", "parsed", "general_decode_out"]
        },
        "eventQueue": {
            "name": "eventQueue",
            "queue": "eventQueue",
            "exchange": "exchange",
            "attributes": ["publish", "event"]
        }
    }
}
----

= Environment variables

```
RABBITMQ_HOST -required; example:rabbit-host
RABBITMQ_PORT -required; example:5672
RABBITMQ_VHOST -required; example:some-virtual-host
RABBITMQ_USER -required; example:some-user
RABBITMQ_PASS -required; example:some-pass
EVENT_STORE_HOST -required; example:event-storage-host
EVENT_STORE_PORT -required; example:8080
CODEC_DICTIONARY -required; example:some-dictionary.xml
DECODER_PARAMETERS - required; example:{"in":{"exchangeName":"demo_exchange","queueName":"decode_in_raw"},"out":{"filters":[{"exchangeName":"demo_exchange","queueName":"decode_out_target_1","filterType":"sessionAlias","parameters":{"sessionAlias":"target_1_session"}},{"exchangeName":"demo_exchange","queueName":"decode_out_target_2","filterType":"sessionAlias","parameters":{"sessionAlias":"target_2_session"}}]}}
ENCODER_PARAMETERS - required; example:{"in":{"exchangeName":"demo_exchange","queueName":"encode_in_raw"},"out":{"filters":[{"exchangeName":"demo_exchange","queueName":"encode_out_target_1","filterType":"sessionAlias","parameters":{"sessionAlias":"target_1_session"}},{"exchangeName":"demo_exchange","queueName":"encode_out_target_2","filterType":"sessionAlias","parameters":{"sessionAlias":"target_2_session"}}]}}
CODEC_CLASS_NAME - required; example:some.codec.ClassName
GENERAL_EXCHANGE_NAME - optional; default value: default_general_exchange;
GENERAL_DECODE_IN_QUEUE - optional; default value: default_general_decode_in;
GENERAL_DECODE_OUT_QUEUE - optional; default value: default_general_decode_in;
GENERAL_ENCODE_IN_QUEUE - optional; default value: default_general_decode_in;
GENERAL_ENCODE_OUT_QUEUE - optional; default value: default_general_decode_in;
```

Decoder or encoder parameters may be omitted, but no both).
In that case codec will be do decode or encode only.

= Filter types

sessionAlias:: filtering by session_alias value, has one parameter sessionAlias; for example: "parameters":{"sessionAlias":"some_session"}
directions:: filtering by direction value, has list parameter; for example: "parameters": { "directions": ["FIRST", "SECOND"]}
directions:: filtering by direction value, has list parameter; for example: "parameters": { "directions": ["FIRST", "SECOND"]}
messageType:: filtering by message type; for example: "parameters":{ "messageType": "SomeMessageName"}



