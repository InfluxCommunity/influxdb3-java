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
package org.influxdb.v3.service;

import java.time.Instant;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.influxdb.v3.reading.EnvReading;
import org.influxdb.v3.sensor.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;

@Service
public class ReadingsService {

    Logger logger = LoggerFactory.getLogger(ReadingsService.class);

    @Value("${readings.query1}")
    String query1;

    InfluxDBClient influxDBClientBase;

    RetryTemplate retryTemplate;

    @Autowired
    public ReadingsService(final InfluxDBClient influxDBClient,
                           final @Qualifier("readsTemplate") RetryTemplate retryTemplateReads) {
        logger.debug("instantiating ReadingsService");
        this.influxDBClientBase = influxDBClient;
        this.retryTemplate = retryTemplateReads;
    }

    public Stream<EnvReading> getAllReadings() {
        System.out.println("DEBUG query1 " + query1);
        return this.retryTemplate.execute(context -> {
            logger.info("getting all readings");
            logger.info("RetryContext {}", context);
            return buildEnvReadings(influxDBClientBase.queryPoints(query1));
        });
    }

    public Stream<PointValues> getAllReadingsAsPV() {
            return this.retryTemplate.execute(context -> {
                logger.info("getting all readings as PointValues");
                logger.info("RetryContext {}", context);
                return influxDBClientBase.queryPoints(query1);
            });
    }

    public Stream<Object[]> getAllReadingsAsObj() {
        return this.retryTemplate.execute(context -> {
            logger.info("getting all readings as Object");
            logger.info("RetryContext {}", context);
            return influxDBClientBase.query(query1);
        });
    }

    Stream<EnvReading> buildEnvReadings(final Stream<PointValues> pointValuesStream) {
        return pointValuesStream.map(e -> {
            double temp = e.getFloatField("temp") == null ? 0 : e.getFloatField("temp");
            double humid = e.getFloatField("humid") == null ? 0 : e.getFloatField("humid");
            double press = e.getFloatField("press") == null ? 0 : e.getFloatField("press");
            Number timestamp = e.getTimestamp() == null ? 0 : e.getTimestamp();
            return new EnvReading(new Sensor(e.getTag("name"), e.getTag("model"), e.getTag("id")),
                temp, humid, press, parseTimestamp(timestamp));
        });
    }

    private static Instant parseTimestamp(final @Nonnull Number timestamp) {
        return Instant.ofEpochMilli(timestamp.longValue() / 1_000_000);
    }
}
