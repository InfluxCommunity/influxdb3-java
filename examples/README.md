# Examples

> :warning: The examples depends on the "influxdb3-java" module and this module should be built first by running "mvn install" in the root directory.
> :warning: Some JDK internals must be exposed by adding `--add-opens=java.base/java.nio=ALL-UNNAMED` to your JVM arguments.

## Basic

- [IOxExample](src/main/java/com/influxdb/v3/IOxExample.java) - How to use write and query data from InfluxDB IOx

## RetryExample

- [RetryExample](src/main/java/com/influxdb/v3/RetryExample.java) - How to catch an `InfluxDBApiHttpException` to then retry making write requests.

### Command line run

- Set environment variables.

```bash

export INFLUX_HOST=<INFLUX_CLOUD_HOST_URL>
export INFLUX_TOKEN=<ORGANIZATION_TOKEN>
export INFLUX_DATABASE=<TARGET_DATABASE>

```

- Run with maven

```bash
mvn compile exec:java -Dexec.main="com.influxdb.v3.RetryExample"
```

- Repeat previous step to force an HTTP 429 response and rewrite attempt.
