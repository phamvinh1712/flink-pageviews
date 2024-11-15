FROM maven:3.8.3-openjdk-11 AS deps
WORKDIR /opt/app

COPY pom.xml .

RUN mvn -B -e clean verify --fail-never

FROM maven:3.8.3-openjdk-11 AS build
WORKDIR /opt/app
COPY --from=deps /root/.m2 /root/.m2
COPY --from=deps /opt/app/ /opt/app
COPY src /opt/app/src

RUN mvn -B -e clean package -DskipTests

FROM flink:1.19.1-java11

ARG FLINK_VERSION=1.19.1
ENV FLINK_LIB_DIR=/opt/flink/lib
ENV FLINK_DIR=/opt/flink

WORKDIR /opt/app
COPY --from=build /opt/app/target/flink-pageviews-*.jar $FLINK_LIB_DIR/flink-pageviews.jar

WORKDIR /opt/flink
