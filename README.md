# How it works

The th2 Codec component is responsible for encoding and decoding the messages. It is operating two instances of encoder/decoder pairs, one for operational purposes and one for general conversion.

Encoding and decoding is performed according to the scheme "one input queue and one or more output queues". Output queues can have filters, depending on which the output of the encoder and decoder can be partially or completely filtered out. The metadata of the message and its fields are used as filter parameters.

One instance of the codec implements the logic for encoding and decoding one protocol of one version. Version-specific protocol messages are described in a separate xml file called a "dictionary".
Codec operates with arrays of messages (parsed batch to raw batch in case of encoding and raw batch to parsed batch upon decoding).

# Running

To start a codec it is required to place external codec api implementation jar with dependencies to 'codec_implementation' folder.

# Configuration

Codec has got four types of connection: stream and general for encode and decode functions.

* stream encode / decode connections are used for work 24 / 7
* general encode / decode connections are used for requests on demand

Messages in stream and general connections are never mixed. 

## Requried pins

Every type of connections is presented two subscribe and publish pins. 
Configuration should include at least one at a time pins for every types (minimal number of them is 8)

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
metadata:
  name: codec
spec:
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