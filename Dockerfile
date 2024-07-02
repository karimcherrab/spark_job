# Use an appropriate base image with Java and Maven pre-installed for building
FROM maven:3.8.5-openjdk-11-slim AS build

# Set the working directory in the container
WORKDIR /app

# Copy the Maven POM file to download dependencies
COPY pom.xml .

# Download dependencies. This step is cached unless your pom.xml changes.
RUN mvn dependency:go-offline

# Copy the rest of your application's source code
COPY src ./src

# Build your application
RUN mvn package

# Stage 2: Use a lighter base image to run the application
FROM openjdk:11-jre-slim

# Set the working directory in the container
WORKDIR /app

# Copy the built JAR file from the build stage
COPY --from=build /app/target/spa-1.0-SNAPSHOT.jar ./app.jar

# Command to run your application
CMD ["java", "-cp", "app.jar", "org.example.JobScheduler"]

# Additional dependencies needed by Jackson Databind
RUN apt-get update && \
    apt-get install -y libjackson-json-java && \
    rm -rf /var/lib/apt/lists/*

# Set the classpath to include Jackson libraries if necessary
ENV CLASSPATH /usr/share/java/jackson-core.jar:/usr/share/java/jackson-databind.jar:$CLASSPATH

# Command to run your application
