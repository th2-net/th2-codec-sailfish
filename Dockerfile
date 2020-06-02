FROM openjdk:12-alpine

WORKDIR /home
COPY ./ .
ENTRYPOINT ["/home/th2-codec/bin/th2-codec", "--config-path=config.yml"]
