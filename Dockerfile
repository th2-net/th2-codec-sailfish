FROM openjdk:12-alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_VHOST=th2 \
    RABBITMQ_USER="" \
    RABBITMQ_PASS="" \
    EVENT_STORE_HOST=event-store \
    EVENT_STORE_PORT=30003 \
    CODEC_DICTIONARY="" \
    CODEC_CLASS_NAME=someClassName
WORKDIR /home
COPY ./ .
ENTRYPOINT ["/home/th2-codec/bin/th2-codec", "--sailfish-codec-config-path=codec_config.yml" , "--config-path=config.yml"]
