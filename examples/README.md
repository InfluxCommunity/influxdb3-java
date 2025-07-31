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

## Durable example

This example illustrates one approach to making sure clients, once initialized, are long-lived and reused.

The underlying write (HTTP/REST) and query (Apache arrow Flight/GRPC) transports are designed to be robust and to be able to recover from most errors.  The InfluxDBClient query API is based on GRPC stubs and channels.  [GRPC best practices](https://grpc.io/docs/guides/performance/) recommends reusing them and their resources for the life of an application if at all possible.  Unnecessary frequent regeneration of InfluxDBClient instances is wasteful of system resources.  Recreating the query transport means fully recreating a GRPC channel, its connection pool and its management API.  Fully recreating a client only to use it for renewed querying means recreating an unused write transport alongside the query transport.  This example attempts to show a more resource friendly use of the API.

- [DurableExample](src/main/java/com/influxdb/v3/durable/DurableExample.java)
- [InfluxClientPool](src/main/java/com/influxdb/v3/durable/InfluxClientPool.java)

