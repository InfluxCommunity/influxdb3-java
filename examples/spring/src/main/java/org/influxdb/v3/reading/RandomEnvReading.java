package org.influxdb.v3.reading;

import org.influxdb.v3.sensor.Sensor;

public class RandomEnvReading {

    public static EnvReading genReading(Sensor sensor){
        return new EnvReading(sensor,
            (Math.random() * 40.0) + (Math.random() * 40.0) - 20.0,
            (Math.random() * 60) + (Math.random() * 40),
            Math.random() * 8.0 + 26.0
            );
    }
}
