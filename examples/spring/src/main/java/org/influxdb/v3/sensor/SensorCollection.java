package org.influxdb.v3.sensor;

import java.util.List;
import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;

public class SensorCollection {

    List<Sensor> sensors;

    @Autowired
    public SensorCollection(final @Nonnull List<Sensor> sensors) {
        this.sensors = sensors;
    }

    public List<Sensor> getSensors() {
        return sensors;
    }

    public void setSensors(final @Nonnull List<Sensor> sensors) {
        this.sensors = sensors;
    }
}
