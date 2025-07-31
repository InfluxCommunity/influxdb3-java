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
package com.influxdb.v3.durable;

import java.time.Instant;

import com.influxdb.v3.client.Point;

/**
 * A generic Sensor.
 */
public class Sensor {

  long id;
  String name;
  String model;
  String location;

  public Sensor(final String name, final String model, final String location) {
    this.id = Math.round(Math.random() * 1000000);
    this.name = name;
    this.model = model;
    this.location = location;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getModel() {
    return model;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return String.format("[%d] name: %s model: %s location: %s", id, name, model, location);
  }

  /**
   * Creates random data for illustrative or testing purposes only.
   *
   * @param timestamp - a timestamp for the data point.  If null returns now.
   * @return A Sensor.DataPoint
   */
  public Sensor.DataPoint randomPoint(final Instant timestamp) {
    return new Sensor.DataPoint(this, randomAccel(), randomVel(), randomBearing(),
      randomAccel(), randomVel(), randomBearing(), timestamp);
  }

  /**
   * Creates a random data point with Instant.now().
   *
   * @return A Sensor.DataPoint.
   */
  public Sensor.DataPoint randomPoint() {
    return randomPoint(Instant.now());
  }

  private static double randomAccel() {
    return Math.random();
  }

  private static double randomVel() {
    return Math.random() + Math.random();
  }

  private static double randomBearing() {
    return Math.random() * 360.0;
  }


  public static class DataPoint {
    Sensor sensor;

    double hAccel;
    double hVel;
    double hBearing;
    double vAccel;
    double vVel;
    double vBearing;
    Instant timestamp;

    String lpFormat = "%s,id=%s,location=%s,model=%s,name=%s "
      + "hAccel=%.6f,hBearing=%.6f,hVel=%.6f,vAccel=%.6f,vBearing=%.6f,vVel=%.6f %d";

    /**
     * Used for illustrating client fail over when incorrect line protocol values are sent.
     */
    String lpFormatBroken = "%s,id=%s,location=%s,model=%s,name=%s "
      + "hAccel=,hBearing=%.6f,hVel=%.6f,vAccel=%.6f,vBearing=%.6f,vVel=%.6f %d";


    public DataPoint(final Sensor sensor,
                     final double hAccel,
                     final double hVel,
                     final double hBearing,
                     final double vAccel,
                     final double vVel,
                     final double vBearing,
                     final Instant timestamp) {
      this.sensor = sensor;
      this.hAccel = hAccel;
      this.hVel = hVel;
      this.hBearing = hBearing;
      this.vAccel = vAccel;
      this.vVel = vVel;
      this.vBearing = vBearing;
      this.timestamp = timestamp == null ? Instant.now() : timestamp;
    }

    @Override
    public String toString() {

      return String.format("%s - hAccel: %.3f, hVel: %.3f, hBearing: %.3f, vAccel: %.3f, vVel: %.3f, vBearing: %.3f %s",
        sensor, hAccel, hVel, hBearing, vAccel, vVel, vBearing, timestamp);
    }

    /**
     * Converts a SensorDataPoint to an InfluxDBClient v3 Point.
     *
     * @return - an InfluxDBClient v3 Point.
     */
    public Point toPoint() {
      return Point.measurement(sensor.getClass().getSimpleName().toLowerCase())
        .setTag("name", sensor.getName())
        .setTag("model", sensor.getModel())
        .setTag("location", sensor.getLocation())
        .setTag("id", Long.toString(sensor.getId()))
        .setFloatField("hAccel", hAccel)
        .setFloatField("hVel", hVel)
        .setFloatField("hBearing", hBearing)
        .setFloatField("vAccel", vAccel)
        .setFloatField("vVel", vVel)
        .setFloatField("vBearing", vBearing)
        .setTimestamp(timestamp);
    }

    /**
     * Convert the SensorDataPoint to a valid Line protocol string.
     *
     * @return - a valid Line protocol string.
     */
    public String toLP() {
      long nanos = timestamp.getEpochSecond() * 1_000_000_000 + timestamp.getNano();
      return String.format(lpFormat,
        sensor.getClass().getSimpleName().toLowerCase(),
        sensor.getId(),
        sensor.getLocation(),
        sensor.getModel(),
        sensor.getName(),
        hAccel,
        hBearing,
        hVel,
        vAccel,
        vBearing,
        vVel,
        nanos
      );
    }

    /**
     * To be used to illustrate how InfluxDBClient recovers from
     * submitting incorrect data to InfluxDB.
     *
     * @return - an invalid Line protocol string.
     */
    public String toLPBroken() {
      long nanos = timestamp.getEpochSecond() * 1_000_000_000 + timestamp.getNano();
      return String.format(lpFormatBroken,
        sensor.getClass().getSimpleName().toLowerCase(),
        sensor.getId(),
        sensor.getLocation(),
        sensor.getModel(),
        sensor.getName(),
        hBearing,
        hVel,
        vAccel,
        vBearing,
        vVel,
        nanos
      );
    }
  }
}
