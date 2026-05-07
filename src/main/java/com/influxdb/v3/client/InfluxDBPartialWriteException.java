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

import java.net.http.HttpHeaders;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * HTTP exception for partial write errors returned by InfluxDB 3 write endpoint.
 * Contains parsed line-level write errors so callers can decide how to handle failed lines.
 */
public class InfluxDBPartialWriteException extends InfluxDBApiHttpException {

    private final List<LineError> lineErrors;

    /**
     * Construct a new InfluxDBPartialWriteException.
     *
     * @param message    detail message
     * @param headers    response headers
     * @param statusCode response status code
     * @param lineErrors line-level errors parsed from response body
     */
    public InfluxDBPartialWriteException(
            @Nullable final String message,
            @Nullable final HttpHeaders headers,
            final int statusCode,
            @Nonnull final List<LineError> lineErrors) {
        super(message, headers, statusCode);
        this.lineErrors = List.copyOf(lineErrors);
    }

    /**
     * Line-level write errors.
     *
     * @return immutable list of line errors
     */
    @Nonnull
    public List<LineError> lineErrors() {
        return lineErrors;
    }

    /**
     * Represents one failed line from a partial write response.
     */
    public static final class LineError {

        private final Integer lineNumber;
        private final String errorMessage;
        private final String originalLine;

        /**
         * @param lineNumber  line number in the write payload; may be null if not provided by server
         * @param errorMessage line-level error message
         * @param originalLine original line protocol row; may be null if not provided by server
         */
        public LineError(@Nullable final Integer lineNumber,
                         @Nonnull final String errorMessage,
                         @Nullable final String originalLine) {
            this.lineNumber = lineNumber;
            this.errorMessage = errorMessage;
            this.originalLine = originalLine;
        }

        /**
         * @return line number or null if server didn't provide it
         */
        @Nullable
        public Integer lineNumber() {
            return lineNumber;
        }

        /**
         * @return line-level error message
         */
        @Nonnull
        public String errorMessage() {
            return errorMessage;
        }

        /**
         * @return original line protocol row or null if server didn't provide it
         */
        @Nullable
        public String originalLine() {
            return originalLine;
        }
    }
}
