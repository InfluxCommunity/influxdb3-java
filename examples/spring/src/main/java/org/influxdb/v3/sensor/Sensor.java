package org.influxdb.v3.sensor;

import java.util.Map;

public class Sensor {

    String name;
    String model;
    String id;

    public Sensor(final String name,
                  final String model,
                  final String id) {
        this.name = name;
        this.model = model;
        this.id = id;
    }

    public Map<String, String> toTags() {
        return Map.of("name", name, "model", model, "id", id);
    }

    @Override
    public String toString() {
        return String.format("name: %s, model: %s, id: %s", name, model, id);
    }
}
