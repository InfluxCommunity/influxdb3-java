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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.influxdb.v3.reading.RandomEnvReading;
import org.influxdb.v3.sensor.Sensor;
import org.influxdb.v3.sensor.SensorCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;

@Service
public class PersistService {

    Logger logger = LoggerFactory.getLogger(PersistService.class);

    InfluxDBClient influxDBClientBase;

    @Value("${influxdb.measurement}")
    String measurement;

    RetryTemplate retryTemplate;

    @Autowired
    public PersistService(final InfluxDBClient influxDBClient,
                          final @Qualifier("writesTemplate")RetryTemplate retryTemplateWrites) {
        this.influxDBClientBase = influxDBClient;
        this.retryTemplate = retryTemplateWrites;
    }

    public void persistDataRandom(final SensorCollection sensors, final int count, final Duration interval) {

        this.retryTemplate.execute(context -> {
            logger.info("persistDataRandom " + count + " sensor sets at interval: " + interval);
            logger.info("context {}", context);
                Instant current = Instant.now().minus(Duration.ofMillis(count * interval.toMillis()));
                Instant end = Instant.now();
                int currentCount = 0;
                while (currentCount < count) {
                    List<Point> points = new ArrayList<>();
                    for (Sensor sensor : sensors.getSensors()) {
                        Point reading = RandomEnvReading.genReading(sensor).toPoint(measurement, current);
                        points.add(reading);
                        logger.info("reading {}", reading.toLineProtocol());
                    }
                    influxDBClientBase.writePoints(points);
                    current = current.plus(interval);
                    currentCount++;
                }
                return null;
        });
    }
}
