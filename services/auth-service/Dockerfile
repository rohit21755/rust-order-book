# syntax=docker/dockerfile:1.6
# ---- Stage 1: build ----
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -e -DskipTests dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -e -DskipTests package && \
    cp target/auth-service-*.jar /workspace/app.jar

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
USER app
EXPOSE 8081
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx512m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s \
    CMD wget -qO- http://localhost:8081/actuator/health/liveness || exit 1
