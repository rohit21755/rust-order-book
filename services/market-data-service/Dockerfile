# syntax=docker/dockerfile:1.6
# ---- Stage 1: build shared libs + market-data-service ----
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# shared-lib
COPY services/shared-lib/pom.xml shared-lib/pom.xml
COPY services/shared-lib/src shared-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-lib/pom.xml -DskipTests install

# shared-kafka-lib
COPY services/shared-kafka-lib/pom.xml shared-kafka-lib/pom.xml
COPY services/shared-kafka-lib/src shared-kafka-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-kafka-lib/pom.xml -DskipTests install

# shared-grpc-lib (protoc downloaded by plugin)
COPY services/shared-grpc-lib/pom.xml shared-grpc-lib/pom.xml
COPY services/shared-grpc-lib/src shared-grpc-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-grpc-lib/pom.xml -DskipTests install

COPY services/market-data-service/pom.xml market-data-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f market-data-service/pom.xml -DskipTests dependency:go-offline
COPY services/market-data-service/src market-data-service/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f market-data-service/pom.xml -DskipTests package && \
    cp market-data-service/target/market-data-service-*.jar /workspace/app.jar

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
USER app
EXPOSE 8083
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx768m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s \
    CMD wget -qO- http://localhost:8083/actuator/health/liveness || exit 1
