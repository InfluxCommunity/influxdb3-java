package com.influxdb.v3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;


/**
 * This examples shows how to catch certain HttpExceptions and leverage their status code and header
 * values to attempt to rewrite data refused by the server.  It attempts to exceed the maximum allowable writes
 * for an unpaid Influx cloud account.
 * <p>
 * A First run with 100,000 records should succeed. but a follow-up run within a couple of minutes
 * should result in a refusal with the HTTP status code 429 "Too Many Requests"
 * <p>
 * See the examples README.md for more information.
 */
public final class RetryExample {

  private static final java.util.logging.Logger LOG = Logger.getLogger(RetryExample.class.getName());

  private RetryExample() { }

  static int retries = 0;

  public static String resolveProperty(final String property, final String fallback) {
    return System.getProperty(property, System.getenv(property)) == null
      ? fallback : System.getProperty(property, System.getenv(property));
  }

  public static void main(final String[] args) throws Exception {

    String host = resolveProperty("INFLUX_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com");
    String token = resolveProperty("INFLUX_TOKEN", "my-token");
    String database = resolveProperty("INFLUX_DATABASE", "my-database");

    Instant now = Instant.now();
    int dataSetSize = 100000;

    LOG.info(String.format("Preparing to write %d records.\n", dataSetSize));

    List<BatteryPack> packs = new ArrayList<>();
    List<Point> points = new ArrayList<>();
    for (int i = 0; i < dataSetSize; i++) {
      packs.add(BatteryPack.createRandom());
      points.add(packs.get(i).toPoint(now.minus((dataSetSize - i) * 10, ChronoUnit.MILLIS)));
    }

    retries = 1;
    try (InfluxDBClient client = InfluxDBClient.getInstance(host, token.toCharArray(), database)) {
      writeWithRetry(client, points, retries);
    }
  }

  /**
   * Resolves the retry value in milliseconds.
   *
   * @param retryVal - retry value from the HTTP header "retry-after"
   * @return - wait interval in milliseconds
   */
  private static long resolveRetry(final String retryVal) {
    try {
      return (Long.parseLong(retryVal) * 1000);
    } catch (NumberFormatException nfe) {
      try {
        Instant now = Instant.now();
        Instant retry = LocalDateTime.parse(retryVal).toInstant(ZoneOffset.UTC);
        return retry.toEpochMilli() - now.toEpochMilli();
      } catch (DateTimeParseException dtpe) {
        throw new RuntimeException("Unable to parse retry time: " + retryVal, dtpe);
      }
    }
  }

  /**
   * Helper method to be called recursively in the event a retry is required.
   *
   * @param client - InfluxDBClient used to make the request.
   * @param points - Data to be written.
   * @param retryCount - Number of times to retry write requests.
   * @throws InterruptedException - if wait is interrupted.
   */
  private static void writeWithRetry(final InfluxDBClient client,
                                     final List<Point> points,
                                     final int retryCount) throws InterruptedException {
    try {
      client.writePoints(points);
      LOG.info(String.format("Succeeded on write %d\n", (retries - retryCount) + 1));
    } catch (InfluxDBApiHttpException e) {
      if ((e.statusCode() == 429 || e.statusCode() == 503)
        && e.getHeader("retry-after") != null
        && retryCount > 0) {
        long wait = resolveRetry(e.getHeader("retry-after").get(0)) + 1000;
        LOG.warning(
          String.format("Failed to write with HTTP %d waiting %d secs to retry.\n", e.statusCode(), wait / 1000)
        );
        keepBusy(wait);
        LOG.info(String.format("Write attempt %d",  ((retries - (retryCount - 1)) + 1)));
        writeWithRetry(client, points, retryCount - 1);
      } else {
        LOG.severe(String.format("Failed to write in %d tries.  Giving up with HTTP %d\n",
          retries,
          e.statusCode()));
      }
    }
  }

  /**
   * Convenience method for a nicer wait experience.
   *
   * @param wait interval to wait before retrying.
   * @throws InterruptedException in the event wait is interrupted.
   */
  private static void keepBusy(final long wait) throws InterruptedException {
    long ttl = Instant.now().toEpochMilli() + wait;
    String[] spinner = {"-", "\\", "|", "/"};
    int count = 0;
    while (Instant.now().toEpochMilli() < ttl) {
      System.out.printf("\r%s", spinner[count++ % 4]);
      System.out.flush();
      TimeUnit.MILLISECONDS.sleep(500);
    }
    System.out.println();
  }

  /**
   * An example data type roughly modeling battery packs in an EV.
   */
  static class BatteryPack {
    String id;
    String vehicle;
    int emptyCells;
    int cellRack;
    double percent;

    public BatteryPack(final String id,
                       final String vehicle,
                       final int emptyCells,
                       final int cellRack,
                       final double percent) {
      this.id = id;
      this.vehicle = vehicle;
      this.emptyCells = emptyCells;
      this.cellRack = cellRack;
      this.percent = percent;
    }

    public Point toPoint(final Instant time) {
      return Point.measurement("bpack")
        .setTag("id", id)
        .setField("vehicle", vehicle)
        .setField("emptyCells", emptyCells)
        .setField("totalCells", cellRack)
        .setField("percent", percent)
        .setTimestamp(time);
    }

    static List<String> idSet = List.of(UUID.randomUUID().toString(),
      UUID.randomUUID().toString(),
      UUID.randomUUID().toString(),
      UUID.randomUUID().toString(),
      UUID.randomUUID().toString());

    static List<String> vehicleSet = List.of(
      "THX1138",
      "VC81072",
      "J007O981",
      "NTSL1856",
      "TOURDF24"
      );

    static List<Integer> cellsSet = List.of(100, 200, 500);

    public static BatteryPack createRandom() {
      int index = (int) Math.floor(Math.random() * 5);
      int totalCells = cellsSet.get((int) Math.floor(Math.random() * 3));
      int emptyCells = (int) Math.floor(Math.random() * totalCells);
      double percent = ((((double) emptyCells / (double) totalCells)) * 100);
      percent += Math.random() * ((100 - percent) * 0.15);
      percent = 100 - percent;
      return new BatteryPack(idSet.get(index), vehicleSet.get(index), emptyCells, totalCells, percent);
    }

    @Override
    public String toString() {
      return String.format("id: %s, vehicle: %s, cellRack: %d, emptyCells: %d, percent: %f",
        id, vehicle, cellRack, emptyCells, percent);
    }
  }
}
