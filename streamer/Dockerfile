# Stage 1: Build the application using Gradle
FROM gradle:8.4.0-jdk17-alpine as builder

# Set working directory
WORKDIR /app

# Copy only the files needed for dependency resolution
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle gradle
COPY src src

# Download dependencies (this layer will be cached unless build.gradle changes)
RUN gradle dependencies

# Build the application
RUN gradle build --no-daemon -x test

# Stage 2: Create the final lightweight runtime image
FROM eclipse-temurin:17-jre-alpine

# Set working directory
WORKDIR /app

# Copy the built artifact from the builder stage
COPY --from=builder /app/build/libs/*SNAPSHOT.jar app.jar

# Set environment variables
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"
ENV SPRING_PROFILES_ACTIVE="prod"

# Expose the port your app runs on
EXPOSE 8080

# Run the application
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /app/app.jar"]
