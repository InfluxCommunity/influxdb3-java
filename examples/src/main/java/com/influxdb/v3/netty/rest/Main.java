package com.influxdb.v3.netty.rest;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.config.ClientConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;

import javax.net.ssl.SSLException;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class Main {
    public static void main(String[] args) throws URISyntaxException, SSLException, ExecutionException, InterruptedException {
        ClientConfig clientConfig = configCloud();

        var testId = UUID.randomUUID().toString();
        // Temporarily use `ClientConfig` as a constructor argument.
        try (RestClient restClient = new RestClient(clientConfig)) {

            // Get server version.
            System.out.println("Server version: " + restClient.getServerVersion());

            // Write data
            System.out.println("Write data with testId " + testId);
            var p = Point.measurement("cpu_sonnh")
                    .setTag("host", "server1")
                    .setField("usage_idle", 90.0f)
                    .setField("testId", testId);
            var lineProtocol = p.toLineProtocol();
            restClient.write(lineProtocol);

            // Read data
            System.out.println("Read data with testId " + testId);
            String query = String.format("SELECT * FROM \"cpu_sonnh\" WHERE \"testId\" = '%s'", testId);
            InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
            var stream = influxDBClient.queryPoints(query);
            stream.findFirst().ifPresent(pointValues -> System.out.println("Field usage_idle: " + pointValues.getField("usage_idle")));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ClientConfig configCloud() {
        String url = System.getenv("TESTING_INFLUXDB_URL");
        String token = System.getenv("TESTING_INFLUXDB_TOKEN");
        String database = System.getenv("TESTING_INFLUXDB_DATABASE");
        return new ClientConfig.Builder()
                .host(url)
                .token(token.toCharArray())
                .database(database)
                .build();
    }

    // This is a docker container ran from scripts/influxdb-setup.sh, get the token and database, url from that script
    public static ClientConfig configLocal() {
        String url = "localhost";
        String token = "apiv3_sMYBS-vRxl6UDMylb7m2u64G6R7g61jlGL76XnUJY3EaN4MD0tZd4DZOBhe6j-dYtoVhrC6PqGgI9Xiv8d3Psw";
        String database = System.getenv("bucket0");
        return new ClientConfig.Builder()
                .host(url)
                .token(token.toCharArray())
                .database(database)
                .build();
    }
}
