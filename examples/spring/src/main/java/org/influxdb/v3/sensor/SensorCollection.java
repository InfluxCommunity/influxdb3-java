package org.influxdb.v3.sensor;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class SensorCollection {

    List<Sensor> sensors;

    @Autowired
    public SensorCollection(List<Sensor> sensors){
        this.sensors = sensors;
    }

    public List<Sensor> getSensors() {
        return sensors;
    }

    public void setSensors(List<Sensor> sensors) {
        this.sensors = sensors;
    }
}
