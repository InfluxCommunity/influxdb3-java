## 1.1.0 [unreleased]

### Features

1. [#209](https://github.com/InfluxCommunity/influxdb3-java/pull/209) Add function return row as a map

## 1.0.0 [2024-12-11]

### Features

1. [#200](https://github.com/InfluxCommunity/influxdb3-java/pull/200): Respect iox::column_type::field metadata when
   mapping query results into values.
   - iox::column_type::field::integer: => Long
   - iox::column_type::field::uinteger: => Long
   - iox::column_type::field::float: => Double
   - iox::column_type::field::string: => String
   - iox::column_type::field::boolean: => Boolean

### Dependencies

1. [#202](https://github.com/InfluxCommunity/influxdb3-java/pull/202): Migrate from `flight-grpc` to `flight-core` package.

## 0.9.0 [2024-08-12]

### Features

1. [#158](https://github.com/InfluxCommunity/influxdb3-java/pull/158): Add InfluxDB Edge (OSS) authentication support.
1. [#163](https://github.com/InfluxCommunity/influxdb3-java/pull/163): Introduces `InfluxDBApiHttpException` to facilitate write retries and error recovery.

### Bug Fixes

1. [#148](https://github.com/InfluxCommunity/influxdb3-java/pull/148): InfluxDB Edge (OSS) error handling
1. [#153](https://github.com/InfluxCommunity/influxdb3-java/pull/153): Parsing timestamp columns

## 0.8.0 [2024-06-24]

### Features

1. [#144](https://github.com/InfluxCommunity/influxdb3-java/pull/133): user-agent header is updated for both REST and gRPC calls.

## 0.7.0 [2024-03-11]

### Features

1. [#107](https://github.com/InfluxCommunity/influxdb3-java/pull/107): Custom headers are also supported for the query (gRPC request)

    ```java
    ClientConfig config = new ClientConfig.Builder()
        .host("https://us-east-1-1.aws.cloud2.influxdata.com")
        .token("my-token".toCharArray())
        .database("my-database")
        .headers(Map.of("X-Tracing-Id", "123"))
        .build();
    
    try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
        //
        // your code here
        //
    } catch (Exception e) {
        throw new RuntimeException(e);
    } 
    ```

1. [#108](https://github.com/InfluxCommunity/influxdb3-java/pull/108): Custom headers can be specified per request (query/write):

    ```java
    ClientConfig config = new ClientConfig.Builder()
        .host("https://us-east-1-1.aws.cloud2.influxdata.com")
        .token("my-token".toCharArray())
        .database("my-database")
        .build();
    
    try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
        //
        // Write with custom headers
        //
        WriteOptions writeOptions = new WriteOptions(
            Map.of("X-Tracing-Id", "852")
        );
        client.writeRecord("mem,tag=one value=1.0", writeOptions);
        
        //
        // Query with custom headers
        //
        QueryOptions queryOptions = new QueryOptions(
            Map.of("X-Tracing-Id", "852")
        );
        Stream<Object[]> rows = client.query("select * from cpu", queryOptions);
   
    } catch (Exception e) {
        throw new RuntimeException(e);
    } 
    ```

## 0.6.0 [2024-03-01]

### Features

1. [#94](https://github.com/InfluxCommunity/influxdb3-java/pull/94): Add support for named query parameters

## 0.5.1 [2024-02-01]

Resync artifacts with Maven Central.

## 0.5.0 [2024-01-30]

### Features

1. [#78](https://github.com/InfluxCommunity/influxdb3-java/pull/78): Default Tags can be used when writing points.

### Bug Fixes

1. [#77](https://github.com/InfluxCommunity/influxdb3-java/pull/77): Serialize InfluxDB response to `PointValues`

## 0.4.0 [2023-11-08]

### Features

1. [#41](https://github.com/InfluxCommunity/influxdb3-java/pull/41): Add structured query support

## 0.3.1 [2023-10-17]

### Bug Fixes

1. [#55](https://github.com/InfluxCommunity/influxdb3-java/pull/55): Iteration over more Arrow streams

## 0.3.0 [2023-10-02]

### Features

1. [#40](https://github.com/InfluxCommunity/influxdb3-java/pull/40): Add client creation from connection string,
environment variables or system properties.

## 0.2.0 [2023-08-11]

### Features

1. [#27](https://github.com/InfluxCommunity/influxdb3-java/pull/27): Add GZIP support
1. [#30](https://github.com/InfluxCommunity/influxdb3-java/pull/30): Add HTTP proxy and custom headers support

### Breaking Changes

1. [#31](https://github.com/InfluxCommunity/influxdb3-java/pull/31): Renamed config types and some options

## 0.1.0 [2023-06-08]

- initial release of new client version
