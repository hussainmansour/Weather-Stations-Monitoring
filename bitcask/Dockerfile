FROM openjdk:21
WORKDIR /app
COPY target/bitcask-1.0-SNAPSHOT.jar /app/bitcask.jar
ENTRYPOINT ["java", "-jar", "bitcask.jar"]