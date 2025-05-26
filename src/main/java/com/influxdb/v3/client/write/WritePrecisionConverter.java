package com.influxdb.v3.client.write;

public class WritePrecisionConverter {

    private WritePrecisionConverter() {
    }

    public static String toV2ApiString(final WritePrecision precision) {
        switch (precision) {
            case NS:
                return "ns";
            case US:
                return "us";
            case MS:
                return "ms";
            case S:
                return "s";
            default:
                throw new IllegalArgumentException("Unsupported precision '" + precision + "'");
        }
    }

    public static String toV3ApiString(final WritePrecision precision) {
        switch (precision) {
            case NS:
                return "nanosecond";
            case US:
                return "microsecond";
            case MS:
                return "millisecond";
            case S:
                return "second";
            default:
                throw new IllegalArgumentException("Unsupported precision '" + precision + "'");
        }
    }
}