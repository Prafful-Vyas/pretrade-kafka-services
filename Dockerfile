# One Dockerfile for both services:  docker build --build-arg SERVICE=order-gateway -t order-gateway .
FROM maven:3.9.9-eclipse-temurin-17 AS build
ARG SERVICE
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY order-gateway/pom.xml order-gateway/
COPY pretrade-risk/pom.xml pretrade-risk/
RUN mvn -B -q dependency:go-offline -DexcludeArtifactIds=common || true
COPY . .
RUN mvn -B -q -pl ${SERVICE} -am package -DskipTests \
 && java -Djarmode=layertools -jar ${SERVICE}/target/${SERVICE}-*.jar extract --destination /layers

FROM eclipse-temurin:17-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/application/ ./
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "org.springframework.boot.loader.launch.JarLauncher"]
