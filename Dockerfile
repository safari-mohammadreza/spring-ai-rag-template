# Build stage
FROM 192.168.10.166:8082/openjdk:21-rc-jdk-bookworm AS build

WORKDIR /app
COPY gradle gradle
COPY build.gradle settings.gradle gradlew ./
RUN sh ./gradlew --no-daemon --refresh-dependencies dependencies
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get install -y ffmpeg

COPY src src
RUN --mount=type=cache,target=/root/.gradle sh ./gradlew build --no-daemon -x test

# Run stage
FROM 192.168.10.166:8082/openjdk:21-rc-jdk-bookworm AS run

# Install FFmpeg
RUN apt update -y && \
    apt install -y ffmpeg && \
    apt clean -y

RUN adduser --system --group app-user
USER app-user
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
