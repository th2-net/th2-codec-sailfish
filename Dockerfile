FROM gradle:7.6-jdk11 AS build
ARG release_version
ARG bintray_user
ARG bintray_key

COPY ./ .
RUN gradle --no-daemon clean build publish \
    -Prelease_version=${release_version} \
    -Pbintray_user=${bintray_user} \
    -Pbintray_key=${bintray_key}
