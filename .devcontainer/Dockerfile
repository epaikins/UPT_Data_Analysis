FROM python:3.11-slim

# Adding Java
COPY --from=openjdk:11-jdk-slim /usr/local/openjdk-11 /usr/local/openjdk-11

ENV JAVA_HOME /usr/local/openjdk-11

RUN pip install --no-cache-dir pyspark pandas dask openpyxl xlrd xlwt