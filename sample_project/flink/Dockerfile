FROM gradle:7.4.0-jdk11 AS build

RUN mkdir -p /app

WORKDIR /app

COPY . /app

RUN gradle build

CMD gradle run
