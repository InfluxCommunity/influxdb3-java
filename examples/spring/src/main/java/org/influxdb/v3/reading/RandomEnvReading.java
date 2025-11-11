package org.influxdb.v3.reading;

import javax.annotation.Nonnull;

import org.influxdb.v3.sensor.Sensor;

public final class RandomEnvReading {

    private RandomEnvReading() { }

    public static EnvReading genReading(final @Nonnull Sensor sensor) {
        return new EnvReading(sensor,
            (Math.random() * 40.0) + (Math.random() * 40.0) - 20.0,
            (Math.random() * 60) + (Math.random() * 40),
            Math.random() * 8.0 + 26.0
            );
    }
}
