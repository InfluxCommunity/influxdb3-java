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
package com.influxdb.v3.client;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractMockServerTest {

    protected String baseURL;
    protected MockWebServer mockServer;

    @BeforeEach
    protected void startMockServer() {

        mockServer = new MockWebServer();
        try {
            mockServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        baseURL = mockServer.url("/").url().toString();
    }

    @AfterEach
    protected void shutdownMockServer() throws IOException {
        mockServer.shutdown();
    }

    @Nonnull
    protected MockResponse createEmptyResponse(final int responseCode) {
        return new MockResponse().setResponseCode(responseCode);
    }

    @Nonnull
    protected MockResponse createResponse(final int responseCode) {

        return createResponseWithHeaders(responseCode, Map.of(
                "Content-Type", "text/csv; charset=utf-8",
                "Date", "Tue, 26 Jun 2018 13:15:01 GMT"
        ));
    }

    @Nonnull
    protected MockResponse createResponseWithHeaders(final int responseCode, final Map<String, String> headers) {

        final MockResponse response = new MockResponse()
                .setResponseCode(responseCode);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            response.addHeader(entry.getKey(), entry.getValue());
        }

        return response;
    }
}
