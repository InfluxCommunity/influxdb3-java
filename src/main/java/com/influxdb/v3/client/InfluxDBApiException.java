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

import javax.annotation.Nullable;

/**
 * The InfluxDBApiException is thrown when an error occurs while interacting with InfluxDB.
 */
public class InfluxDBApiException extends RuntimeException {
    /**
     * Construct a new InfluxDBApiException with the specified detail message.
     *
     * @param message the detail message.
     */
    public InfluxDBApiException(@Nullable final String message) {
        super(message);
    }

    /**
     * Construct a new InfluxDBApiException with the specified cause.
     *
     * @param cause   the cause.
     */
    public InfluxDBApiException(@Nullable final Throwable cause) {
        super(cause);
    }
}
