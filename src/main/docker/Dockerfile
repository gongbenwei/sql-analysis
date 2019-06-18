FROM openjdk:8-jdk-alpine
VOLUME /tmp
ADD sql-analysis-1.0.jar sql-analysis.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/sql-analysis.jar"]