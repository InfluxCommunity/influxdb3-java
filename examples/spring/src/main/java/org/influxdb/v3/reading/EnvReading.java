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
