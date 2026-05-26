# syntax=docker/dockerfile:1.6
# ---- Stage 1: build shared libs + risk-engine ----
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY services/shared-lib/pom.xml shared-lib/pom.xml
COPY services/shared-lib/src shared-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-lib/pom.xml -DskipTests install

COPY services/shared-kafka-lib/pom.xml shared-kafka-lib/pom.xml
COPY services/shared-kafka-lib/src shared-kafka-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-kafka-lib/pom.xml -DskipTests install

COPY services/risk-engine/pom.xml risk-engine/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f risk-engine/pom.xml -DskipTests dependency:go-offline
COPY services/risk-engine/src risk-engine/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f risk-engine/pom.xml -DskipTests package && \
    cp risk-engine/target/risk-engine-*.jar /workspace/app.jar

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
USER app
EXPOSE 8085
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx512m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s \
    CMD wget -qO- http://localhost:8085/actuator/health/liveness || exit 1
