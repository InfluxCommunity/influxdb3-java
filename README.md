<p align="center">
    <img src="duke_logo.png" alt="Duke" width="100px">
</p>
<p align="center">
    <a href="https://repo1.maven.org/maven2/com/influxdb/influxdb3-java/">
        <img src="https://img.shields.io/maven-central/v/com.influxdb/influxdb3-java" alt="Maven Central Badge">
    </a>
    <a href="https://InfluxCommunity.github.io/influxdb3-java/">
        <img src="https://img.shields.io/badge/maven-site-blue" alt="Maven Site">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-java/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-java/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-java/actions/workflows/linter.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-java/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/InfluxCommunity/influxdb3-java/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/InfluxCommunity/influxdb3-java/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/InfluxCommunity/influxdb3-java">
        <img src="https://codecov.io/gh/InfluxCommunity/influxdb3-java/branch/main/graph/badge.svg" alt="Code Cov"/>
    </a>
    <a href="https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3 Java Client

The Java client that provides an easy and convenient way to interact with InfluxDB 3.
This package supports both writing data to InfluxDB and querying data using the FlightSQL client,
which allows you to execute SQL queries against InfluxDB IOx.

We offer this [Getting Started: InfluxDB 3.0 Java Client Library](https://www.youtube.com/watch?v=EFnG7rUDvR4) video for learning more about the library.

> :warning: This client requires Java 11 and is compatible up to and including Java 20.

## Installation

> :warning: Some JDK internals must be exposed by adding `--add-opens=java.base/java.nio=ALL-UNNAMED` to your JVM arguments.

Add the latest version of the client to your project:

### Maven dependency

```xml
<dependency>
    <groupId>com.influxdb</groupId>
    <artifactId>influxdb3-java</artifactId>
    <version>0.7.0</version>
</dependency>
```

### Or when using Gradle

```groovy
dependencies {
    implementation "com.influxdb:influxdb3-java:0.7.0"
}
```

## Usage

To start with the client, import the `com.influxdb.v3.client` package and create a `InfluxDBClient` by:

```java
package com.influxdb.v3;

import java.time.Instant;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.Point;

public class IOxExample {
    public static void main(String[] args) throws Exception {
        String host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        char[] token = "my-token".toCharArray();
        String database = "database";

        try (InfluxDBClient client = InfluxDBClient.getInstance(host, token, database)) {
            // ...
        }
    }
}
```

to insert data, you can use code like this:

```java
//
// Write by Point
//
Point point = Point.measurement("temperature")
        .setTag("location", "west")
        .setField("value", 55.15)
        .setTimestamp(Instant.now().minusSeconds(-10));
client.writePoint(point);

//
// Write by LineProtocol
//
String record = "temperature,location=north value=60.0";
client.writeRecord(record);
```

to query your data, you can use code like this:

```java
//
// Query by SQL
//
System.out.printf("--------------------------------------------------------%n");
System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
System.out.printf("--------------------------------------------------------%n");

String sql = "select time,location,value from temperature order by time desc limit 10";
try (Stream<Object[]> stream = client.query(sql)) {
    stream.forEach(row -> System.out.printf("| %-8s | %-8s | %-30s |%n", row[1], row[2], row[0]));
}

System.out.printf("--------------------------------------------------------%n%n");

//
// Query by parametrized SQL
//
System.out.printf("--------------------Parametrized SQL--------------------%n");
System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
System.out.printf("--------------------------------------------------------%n");

String sqlParams = "select time,location,value from temperature where location=$location order by time desc limit 10";
try (Stream<Object[]> stream = client.query(sqlParams, Map.of("location", "north"))) {
    stream.forEach(row -> System.out.printf("| %-8s | %-8s | %-30s |%n", row[1], row[2], row[0]));
}

System.out.printf("--------------------------------------------------------%n%n");

//
// Query by InfluxQL
//
System.out.printf("-----------------------------------------%n");
System.out.printf("| %-16s | %-18s |%n", "time", "mean");
System.out.printf("-----------------------------------------%n");

String influxQL = "select MEAN(value) from temperature group by time(1d) fill(none) order by time desc limit 10";
try (Stream<Object[]> stream = client.query(influxQL, QueryOptions.INFLUX_QL)) {
    stream.forEach(row -> System.out.printf("| %-16s | %-18s |%n", row[1], row[2]));
}

System.out.printf("-----------------------------------------%n");
```

or use `PointValues` structure with `client.queryPoints`:

```java
System.out.printf("--------------------------------------------------------%n");
System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
System.out.printf("--------------------------------------------------------%n");

//
// Query by SQL into Points
//
String sql1 = "select time,location,value from temperature order by time desc limit 10";
try (Stream<PointValues> stream = client.queryPoints(sql1, QueryOptions.DEFAULTS)) {
    stream.forEach(
        (PointValues p) -> {
            var time = p.getField("time", LocalDateTime.class);
            var location = p.getField("location", String.class);
            var value = p.getField("value", Double.class);

            System.out.printf("| %-8s | %-8s | %-30s |%n", location, value, time);
    });
}

System.out.printf("--------------------------------------------------------%n%n");
```

## Feedback

If you need help, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA)
or [Community Page](https://community.influxdata.com/).

New features and bugs can be reported on GitHub: <https://github.com/InfluxCommunity/influxdb3-java>

## Contribution

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into
the `main` branch.

## License

The InfluxDB 3 Java Client is released under the [MIT License](https://opensource.org/licenses/MIT).
