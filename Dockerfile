# Use an official Maven image to build the application
FROM maven:3.8.1-openjdk-11 AS build

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the pom.xml file and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy the source code and build the application
COPY src ./src
RUN mvn package -DskipTests

# Use an official OpenJDK runtime as a parent image
FROM openjdk:11-jre-slim


# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the built jar from the previous stage
COPY --from=build /usr/src/app/target/spa-1.0-SNAPSHOT.jar ./spa-1.0-SNAPSHOT.jar

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_HOME=C:\\spark-3.5.1-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin

# Define the command to run your app using Spark
CMD ["spark-submit", "--class", "org.example.JobScheduler", "--master", "local[*]", "/usr/src/app/spa-1.0-SNAPSHOT.jar"]
