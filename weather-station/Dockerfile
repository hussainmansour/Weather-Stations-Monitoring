FROM openjdk:21
WORKDIR /app
COPY target/weather-station-1.0-SNAPSHOT.jar /app/weather-station.jar
ENTRYPOINT ["java", "-jar", "weather-station.jar"]