package com.influxdb.v3;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.util.stream.Stream;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

public class ProxyExample {

    public static void main(final String[] args) throws Exception {
        // Run docker-compose.yml file to start Envoy proxy

        URI queryProxyUri = new URI("proxyUrl");
        URI uri = new URI(System.getenv("url"));

        ProxyDetector proxyDetector = (targetServerAddress) -> {
            InetSocketAddress targetAddress = (InetSocketAddress) targetServerAddress;
            if (uri.getHost().equals(targetAddress.getHostString())) {
                return HttpConnectProxiedSocketAddress.newBuilder()
                        .setProxyAddress(new InetSocketAddress(queryProxyUri.getHost(), queryProxyUri.getPort()))
                        .setTargetAddress(targetAddress)
                        .build();
            }
            return null;
        };
        ProxySelector proxy = ProxySelector.of(new InetSocketAddress(queryProxyUri.getHost(), queryProxyUri.getPort()));
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(uri.toString())
                .token(System.getenv("token").toCharArray())
                .database(System.getenv("database"))
                .proxy(proxy)
                .queryApiProxy(proxyDetector)
                .build();

        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        influxDBClient.writePoint(
                Point.measurement("test1")
                        .setField("field", "field1")
        );

        try (Stream<PointValues> stream = influxDBClient.queryPoints("SELECT * FROM test1")) {
            stream.findFirst()
                    .ifPresent(pointValues -> {
                        // do something
                    });
        }
    }

    private ProxyExample(){}
}

