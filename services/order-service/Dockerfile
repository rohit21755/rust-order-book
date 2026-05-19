# syntax=docker/dockerfile:1.6
# ---- Stage 1: build shared-lib + order-service ----
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# shared-lib first (order-service depends on it)
COPY services/shared-lib/pom.xml shared-lib/pom.xml
COPY services/shared-lib/src shared-lib/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f shared-lib/pom.xml -DskipTests install

COPY services/order-service/pom.xml order-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f order-service/pom.xml -DskipTests dependency:go-offline
COPY services/order-service/src order-service/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f order-service/pom.xml -DskipTests package && \
    cp order-service/target/order-service-*.jar /workspace/app.jar

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
USER app
EXPOSE 8082
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx512m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s \
    CMD wget -qO- http://localhost:8082/actuator/health/liveness || exit 1
