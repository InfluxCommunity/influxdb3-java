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
package com.influxdb.v3.client.internal;

import java.time.Duration;

import io.netty.handler.codec.http.HttpMethod;
import okhttp3.mockwebserver.RecordedRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.AbstractMockServerTest;
import com.influxdb.v3.client.config.InfluxDBClientConfigs;

public class RestClientTest extends AbstractMockServerTest {

    private RestClient restClient;

    @AfterEach
    void tearDown() {
        if (restClient != null) {
            restClient.close();
        }
    }

    @Test
    public void baseUrl() {
        restClient = new RestClient(new InfluxDBClientConfigs.Builder().hostUrl("http://localhost:8086").build());
        Assertions
                .assertThat(restClient)
                .extracting("client.config.baseUrl")
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void baseUrlSlashEnd() {
        restClient = new RestClient(new InfluxDBClientConfigs.Builder().hostUrl("http://localhost:8086/").build());
        Assertions
                .assertThat(restClient)
                .extracting("client.config.baseUrl")
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void responseTimeout() {
        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl("http://localhost:8086")
                .responseTimeout(Duration.ofSeconds(13))
                .build());

        Assertions
                .assertThat(restClient)
                .extracting("client.config.responseTimeout")
                .isEqualTo(Duration.ofSeconds(13));
    }

    @Test
    public void allowHttpRedirectsDefaults() {
        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl("http://localhost:8086")
                .build());

        Assertions
                .assertThat(restClient)
                .extracting("client.config.followRedirectPredicate")
                .isNull();
    }

    @Test
    public void authenticationHeader() throws InterruptedException {
        mockServer.enqueue(createResponse());

        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl(baseURL)
                .authToken("my-token")
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeader("Authorization");
        Assertions.assertThat(authorization).isEqualTo("Token my-token");
    }

    @Test
    public void authenticationHeaderNotDefined() throws InterruptedException {
        mockServer.enqueue(createResponse());

        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeader("Authorization");
        Assertions.assertThat(authorization).isNull();
    }

    @Test
    public void userAgent() throws InterruptedException {
        mockServer.enqueue(createResponse());

        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String userAgent = recordedRequest.getHeader("User-Agent");
        Assertions.assertThat(userAgent).startsWith("influxdb3-java/");
    }

    @Test
    public void uri() throws InterruptedException {
        mockServer.enqueue(createResponse());

        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getRequestUrl()).isNotNull();
        Assertions.assertThat(recordedRequest.getRequestUrl().toString()).isEqualTo(baseURL + "ping");
    }

    @Test
    public void allowHttpRedirects() {
        restClient = new RestClient(new InfluxDBClientConfigs.Builder()
                .hostUrl("http://localhost:8086")
                .allowHttpRedirects(true)
                .build());

        Assertions
                .assertThat(restClient)
                .extracting("client.config.followRedirectPredicate")
                .isNotNull();
    }
}
