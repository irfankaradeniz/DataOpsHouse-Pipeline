# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install PySpark
RUN pip install pyspark==3.1.2

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Set environment variables
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Make port 80 available to the world outside this container
EXPOSE 80

# Run your script when the container is launched
CMD ["python", "pipeline.py"]
