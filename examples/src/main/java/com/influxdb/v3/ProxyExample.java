/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.influxdb.v3;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;
import io.netty.handler.proxy.HttpProxyHandler;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.config.NettyHttpClientConfig;

public final class ProxyExample {

    private ProxyExample() {
    }

    public static void main(final String[] args) throws Exception {
        // Run the docker-compose.yml file to start Envoy proxy,
        // or start envoy proxy directly with the command `envoy-c envoy.yaml`

        String proxyUrl = "http://localhost:10000";
        String targetUrl = "http://localhost:8086";
        String username = "username";
        String password = "password";

        NettyHttpClientConfig nettyHttpClientConfig = new NettyHttpClientConfig();

        // Set proxy for write api
        Supplier<HttpProxyHandler> writeApiProxy = () ->
                new HttpProxyHandler(new InetSocketAddress("localhost", 10000), username, password);
        nettyHttpClientConfig.configureChannelProxy(writeApiProxy);

        // Set proxy for query api
        ProxyDetector proxyDetector = createProxyDetector(targetUrl, proxyUrl, username, password);
        nettyHttpClientConfig.configureManagedChannelProxy(proxyDetector);

        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("INFLUXDB_URL"))
                .token(System.getenv("INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("INFLUXDB_DATABASE"))
                .nettyHttpClientConfig(nettyHttpClientConfig)
                .build();

        try (InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig)) {
            String testId = UUID.randomUUID().toString();
            Point point = Point.measurement("My_Home")
                    .setTag("room", "Kitchen")
                    .setField("temp", 12.7)
                    .setField("hum", 37)
                    .setField("testId", testId);
            influxDBClient.writePoint(point);

            String query = String.format("SELECT * FROM \"My_Home\" WHERE \"testId\" = '%s'", testId);
            try (Stream<PointValues> stream = influxDBClient.queryPoints(query)) {
                stream.findFirst().ifPresent(values -> {
                    assert values.getTimestamp() != null;
                    System.out.printf("room[%s]: %s, temp: %3.2f, hum: %d",
                            new java.util.Date(values.getTimestamp().longValue() / 1000000),
                            values.getTag("room"),
                            (Double) values.getField("temp"),
                            (Long) values.getField("hum"));
                });
            }
        }
    }

    public static ProxyDetector createProxyDetector(@Nonnull final String targetUrl, @Nonnull final String proxyUrl,
                                                    @Nullable final String username, @Nullable final String password) {
        URI targetUri = URI.create(targetUrl);
        URI proxyUri = URI.create(proxyUrl);
        return (targetServerAddress) -> {
            InetSocketAddress targetAddress = (InetSocketAddress) targetServerAddress;
            if (targetUri.getHost().equals(targetAddress.getHostString())
                    && targetUri.getPort() == targetAddress.getPort()) {
                return HttpConnectProxiedSocketAddress.newBuilder()
                        .setProxyAddress(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()))
                        .setTargetAddress(targetAddress)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
            }
            return null;
        };
    }
}

