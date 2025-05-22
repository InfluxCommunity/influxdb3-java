package com.influxdb.v3.client.write;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WritePrecisionConverterTest {
    @Test
    void toV2ApiString() {
        Map<WritePrecision, String> testCases = Map.of(
                WritePrecision.NS, "ns",
                WritePrecision.US, "us",
                WritePrecision.MS, "ms",
                WritePrecision.S, "s"
        );

        for (Map.Entry<WritePrecision, String> e : testCases.entrySet()) {
            WritePrecision precision = e.getKey();
            String expectedString = e.getValue();
            String result = WritePrecisionConverter.toV2ApiString(precision);
            assertEquals(expectedString, result, "Failed for precision: " + precision);
        }
    }

    @Test
    void toV3ApiString() {
        Map<WritePrecision, String> tc = Map.of(
                WritePrecision.NS, "nanosecond",
                WritePrecision.US, "microsecond",
                WritePrecision.MS, "millisecond",
                WritePrecision.S, "second"
        );

        for (Map.Entry<WritePrecision, String> e : tc.entrySet()) {
            WritePrecision precision = e.getKey();
            String expectedString = e.getValue();
            String result = WritePrecisionConverter.toV3ApiString(precision);
            assertEquals(expectedString, result, "Failed for precision: " + precision);
        }
    }
}
