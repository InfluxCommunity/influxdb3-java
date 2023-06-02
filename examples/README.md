# Examples

> :warning: The examples depends on the "influxdb3-java" module and this module should be built first by running "mvn install" in the root directory.
> :warning: When using Java 9 or later, some JDK internals must be exposed by adding `--add-opens=java.base/java.nio=ALL-UNNAMED` to your JVM arguments.

## Basic

- [IOxExample](src/main/java/com/influxdb/v3/IOxExample.java) - How to use write and query data from InfluxDB IOx
