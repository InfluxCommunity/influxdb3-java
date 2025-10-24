package org.influxdb.v3.reading;

import com.influxdb.v3.client.Point;
import org.influxdb.v3.sensor.Sensor;

import java.time.Instant;

public class EnvReading {

    Sensor sensor;
    double temperature;
    double humidity;
    double pressure;
    Instant timestamp;

    public EnvReading(Sensor sensor, double temperature, double humidity, double pressure) {
        this.sensor = sensor;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
    }

    public EnvReading(Sensor sensor, double temperature, double humidity, double pressure, Instant timestamp) {
        this.sensor = sensor;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
        this.timestamp = timestamp;
    }


    public Sensor getSensor() {
        return this.sensor;
    }

    public void setSensor(Sensor sensor) {
        this.sensor = sensor;
    }

    public double getTemperature() {
        return this.temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return this.humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }
    public double getPressure() {
        return this.pressure;
    }
    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    public Point toPoint(String measurement, Instant timestamp) {
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
