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
package com.influxdb.v3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;

public final class DownsamplingExample {
    public static void main(String[] args) throws Exception {
        String host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        String token = "my-token";
        String database = "my-database";

        try (InfluxDBClient client = InfluxDBClient.getInstance(host, token.toCharArray(), database)) {
            //
            // Write data
            //
            Point point1 = Point.measurement("stat")
                    .setTag("unit", "temperature")
                    .setField("avg", 24.5)
                    .setField("max", 45.0)
                    .setTimestamp(Instant.now().minusSeconds(-20*60));
            client.writePoint(point1);

            Point point2 = Point.measurement("stat")
                    .setTag("unit", "temperature")
                    .setField("avg", 28.0)
                    .setField("max", 40.3)
                    .setTimestamp(Instant.now().minusSeconds(-10*60));
            client.writePoint(point2);

            Point point3 = Point.measurement("stat")
                    .setTag("unit", "temperature")
                    .setField("avg", 20.5)
                    .setField("max", 49.0)
                    .setTimestamp(Instant.now());
            client.writePoint(point3);

            //
            // Query downsampled data
            //
            String sql = "SELECT\n"
                + "     date_bin('5 minutes', \"time\") as window_start,\n"
                + "     AVG(\"avg\") as avg,\n"
                + "     MAX(\"max\") as max\n"
                + " FROM \"stat\"\n"
                + " WHERE\n"
                + "       \"time\" >= now() - interval '1 hour'\n"
                + " GROUP BY window_start\n"
                + "     ORDER BY window_start ASC;\n"
                    ;


            //
            // Execute downsampling query into pointValues
            //
            try (Stream<PointValues> stream = client.queryPoints(sql)) {
                stream.forEach(
                    (PointValues row) -> {
                        var timestamp = row.getField("window_start", LocalDateTime.class);

                        if (timestamp == null) {
                            return;
                        }

                        System.out.println(timestamp.toString()+": avg is "+row.getFloatField("avg")+", max is "+row.getFloatField("max"));

                        //
                        // write back downsampled date to 'stat_downsampled' measurement
                        //
                        var downsampledPoint = row
                                .asPoint("stat_downsampled")
                                .removeField("window_start")
                                .setTimestamp(timestamp.toInstant(ZoneOffset.UTC));

                        client.writePoint(downsampledPoint);
                });
            }


        }
    }
}
