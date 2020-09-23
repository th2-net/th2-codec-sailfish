FROM gradle:6.6-jdk11 AS build
ARG app_version=0.0.0
COPY ./ .
RUN gradle dockerPrepare -Prelease_version=${app_version}

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
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "--sailfish-codec-config-path=codec_config.yml" , "--config-path=config.yml"]
