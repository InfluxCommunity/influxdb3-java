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
package org.influxdb.v3.reading;

import java.time.Instant;
import javax.annotation.Nonnull;

import org.influxdb.v3.sensor.Sensor;

import com.influxdb.v3.client.Point;

public class EnvReading {

    Sensor sensor;
    double temperature;
    double humidity;
    double pressure;
    Instant timestamp;

    public EnvReading(final @Nonnull Sensor sensor,
                      final double temperature,
                      final double humidity,
                      final double pressure) {
        this.sensor = sensor;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
    }

    public EnvReading(final @Nonnull Sensor sensor,
                      final double temperature,
                      final double humidity,
                      final double pressure,
                      final @Nonnull Instant timestamp) {
        this.sensor = sensor;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
        this.timestamp = timestamp;
    }

    public Point toPoint(final String measurement, final @Nonnull Instant timestamp) {
        if (this.timestamp == null) {
            this.timestamp = timestamp;
        }
        return new Point(measurement)
            .setTags(this.sensor.toTags())
            .setFloatField("temp", this.temperature)
            .setFloatField("humid", this.humidity)
            .setFloatField("press", this.pressure)
            .setTimestamp(timestamp);
    }

    @Override
    public String toString() {
        return String.format("sensor[%s] temp: %f3.3, humid: %f3.3, press: %f3.3, time: %s",
            sensor, temperature, humidity, pressure, timestamp);
    }
}
