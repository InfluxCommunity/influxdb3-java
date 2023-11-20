package com.influxdb.v3.client;

import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PointTest
{
    @Test
    void setMeasurement() {
        Point point = Point.measurement("measurement");
        assertEquals("measurement", point.getMeasurement());

        point.setMeasurement("newMeasurement");
        assertEquals("newMeasurement", point.getMeasurement());
    }

    @Test
    void setTimestamp() {
        Point point = Point.measurement("measurement");

        Instant timestamp = Instant.parse("2023-11-08T12:00:00Z");
        point.setTimestamp(timestamp);
        assertEquals(BigInteger.valueOf(timestamp.getEpochSecond()).multiply(BigInteger.valueOf(1_000_000_000))
                , point.getTimestamp());
    }

    @Test
    void setTags() {
        Point point = Point.measurement("measurement");

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        point.setTags(tags);

        assertEquals("value1", point.getTag("tag1"));
        assertEquals("value2", point.getTag("tag2"));
    }

    @Test
    void setFields() {
        Point point = Point.measurement("measurement");

        point.setField("field1", 42);
        point.setField("field2", "value");
        point.setField("field3", 3.14);

        assertEquals(42L, point.getField("field1"));
        assertEquals("value", point.getField("field2"));
        assertEquals(3.14, point.getField("field3"));
    }

    @Test
    void toLineProtocol() {
        Point point = Point.measurement("measurement")
                .setTag("tag1", "value1")
                .setField("field1", 42);

        String lineProtocol = point.toLineProtocol(WritePrecision.NS);
        assertEquals("measurement,tag1=value1 field1=42i", lineProtocol);
    }

    @Test
    void copy() {
        Point point = Point.measurement("measurement")
                .setTag("tag1", "value1")
                .setField("field1", 42);

        Point copy = point.copy();

        // Ensure the copy is not the same object
        assertNotSame(point, copy);
        // Ensure the values are equal
        assertEquals(point.getMeasurement(), copy.getMeasurement());
        assertEquals(point.getTag("tag1"), copy.getTag("tag1"));
        assertEquals(point.getField("field1"), copy.getField("field1"));
    }
}
