# Use OpenJDK 17 as the base image
FROM openjdk:17-slim

# Install necessary dependencies
RUN apt-get update \
    && apt-get install -y \
        curl \
        libxrender1 \
        libjpeg62-turbo \
        fontconfig \
        libxtst6 \
        xfonts-75dpi \
        xfonts-base \
        xz-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Optional: Uncomment to install wkhtmltopdf if needed
#RUN curl "https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb" -L -o "wkhtmltopdf.deb"
#RUN dpkg -i wkhtmltopdf.deb

# Copy the application JAR file
COPY cb-enrollment-service-0.0.1-SNAPSHOT.jar /opt/

# Optional: Define a health check
#HEALTHCHECK --interval=30s --timeout=30s CMD curl --fail http://localhost:7001/actuator/health || exit 1

# Set the command to run the Java application

CMD ["/bin/bash", "-c", "java", "-XX:+PrintFlagsFinal", "$JAVA_OPTIONS", "-XX:+UnlockExperimentalVMOptions", "-jar", "/opt/cb-enrollment-service-0.0.1-SNAPSHOT.jar"]

