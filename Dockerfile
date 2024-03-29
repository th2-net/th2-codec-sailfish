FROM gradle:7.6-jdk11 AS build
ARG release_version=0.0.0
COPY ./ .
RUN gradle clean build dockerPrepare -Prelease_version=${release_version}

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "--sailfish-codec-config=codec_config.yml"]
