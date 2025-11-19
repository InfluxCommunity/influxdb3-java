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

import io.netty.handler.codec.http.HttpHeaders;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The InfluxDBApiNettyException gets thrown whenever an error status is returned
 * in the HTTP response.  It facilitates recovering from such errors whenever possible.
 */
public class InfluxDBApiNettyException extends InfluxDBApiException {

  /**
   * The HTTP headers associated with the error.
   */
  HttpHeaders headers;
  /**
   * The HTTP status code associated with the error.
   */
  int statusCode;

  /**
   * Construct a new InfluxDBApiNettyException with statusCode and headers.
   *
   * @param message the detail message.
   * @param headers headers returned in the response.
   * @param statusCode statusCode of the response.
   */
  public InfluxDBApiNettyException(
    @Nullable final String message,
    @Nullable final HttpHeaders headers,
    final int statusCode) {
    super(message);
    this.headers = headers;
    this.statusCode = statusCode;
  }

  /**
   * Construct a new InfluxDBApiNettyException with statusCode and headers.
   *
   * @param cause root cause of the exception.
   * @param headers headers returned in the response.
   * @param statusCode status code of the response.
   */
  public InfluxDBApiNettyException(
    @Nullable final Throwable cause,
    @Nullable final HttpHeaders headers,
    final int statusCode) {
    super(cause);
    this.headers = headers;
    this.statusCode = statusCode;
  }

  /**
   * Gets the HTTP headers property associated with the error.
   *
   * @return - the headers object.
   */
  public HttpHeaders headers() {
    return headers;
  }

  /**
   * Helper method to simplify retrieval of specific headers.
   *
   * @param name - name of the header.
   * @return - value matching the header key, or null if the key does not exist.
   */
  public List<String> getHeader(final String name) {
    return headers.getAll(name);
  }

  /**
   * Gets the HTTP statusCode associated with the error.
   * @return - the HTTP statusCode.
   */
  public int statusCode() {
    return statusCode;
  }

}
