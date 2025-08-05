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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

/**
 * The example depends on the "influxdb3-java" module and this module should be built first
 * by running "mvn install" in the root directory.
 * <p>
 * This example illustrates how to reuse InfluxDBClient instances.  The underlying write (REST) and
 * query (apache arrow Flight/GRPC) transports are designed to be robust and long-lived.  Frequent creation or
 * recreation of InfluxDBClients and then discarding them and their underlying transports is
 * inefficient.  GRPC best practices recommends trying to use their channels, on which the InfluxDBClient
 * query transport is based, for the life of an application, if at all possible.  The write transport is
 * also designed to recover from most errors and to be reusable.  This example is one approach to reusing
 * InfluxDBClient instances for as long as possible.
 * <p>
 * At its core this example uses a client pool and four processing threads.  The threads borrow
 * clients from the pool as needed and then return them once they are no longer needed.  Two threads
 * are used for writing data and two additional threads are used for executing queries.
 * One write thread is designed to occasionally force an error response from the server.  Like wise one
 * query thread is designed to occasionally elicit error responses in the GRPC channel.  Even though
 * errors occur in these transactions, the clients involved can continue to be used for later writes
 * and queries.  Furthermore, while four processing threads are running, the pool need only instantiate three
 * clients, if handled properly.
 */
public final class DurableExample {

  static Logger logger = Logger.getLogger(DurableExample.class.getName());

  public static ClientConfig clientConfig;

  private DurableExample() {
  }

  public static void setup() {

    String influxHost = System.getenv("INFLUX_HOST") != null
      ? System.getenv("INFLUX_HOST") : "http://localhost:8181";
    String influxToken = System.getenv("INFLUX_TOKEN") != null
      ? System.getenv("INFLUX_TOKEN") : "my-token";
    String influxDatabase = System.getenv("INFLUX_DATABASE") != null
      ? System.getenv("INFLUX_DATABASE") : "my-db";

    clientConfig = new ClientConfig.Builder()
      .host(influxHost)
      .token(influxToken.toCharArray())
      .database(influxDatabase)
      .build();
  }

  public static void main(final String[] args) {

    setup();

    // A basic control signal
    AtomicBoolean shutdownAll = new AtomicBoolean(false);

    // time to run the example in minutes
    int runTime = 2;

    // a set of sensors as a source of data
    List<Sensor> sensors = List.of(
      new Sensor("Alfa", "Univac51", "libava"),
      new Sensor("Bravo", "Eniac45", "brezina"),
      new Sensor("Charlie", "Ordvac52", "boletice"),
      new Sensor("Delta", "HAL2001", "hradiste"),
      new Sensor("Echo", "BESM68", "brdy")
    );

    // standard query string
    String query = String.format("SELECT * FROM %s ORDER BY time DESC", Sensor.class.getSimpleName());

    // query string to elicit error response
    String badQuery = String.format("SELECT * FOO %s ORDER BY time DESC", Sensor.class.getSimpleName());

    // Set up the autoclosable client pool
    try (InfluxClientPool clientPool = new InfluxClientPool(clientConfig)) {

      // thread controller
      final ExecutorService executors = Executors.newFixedThreadPool(5);

      // an error free write thread
      final Runnable writeOK = () -> {
        int count = 0;
        while (!shutdownAll.get()) {
          List<Point> points = new ArrayList<>();
          for (Sensor sensor : sensors) {
            points.add(sensor.randomPoint().toPoint());
          }

          // borrow then return a client
          InfluxDBClient client = clientPool.borrowClient();
          try {
            logger.info(" [writeTaskPointsOK " + count + "] Writing " + points.size()
              + " points with client " + client.hashCode());
            client.writePoints(points);
          } catch (Exception e) {
            logger.severe(" [writeTaskPointsOK " + count + "] Unexpected Error writing points "
              + e.getMessage());
          } finally {
            clientPool.returnClient(client);
          }

          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
          count++;
        }
        logger.info(" [writeTaskPointsOK] shutting down");
      };

      // An error-prone write thread
      final Runnable writeErrorRecover = () -> {
        // delay start by 2 seconds
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
        int count = 0;
        while (!shutdownAll.get()) {
          List<String> lps = new ArrayList<>();
          for (Sensor sensor : sensors) {
            // every fourth write attempt uses an invalid Line protocol line
            if (count > 0 && count % 4 == 0 && sensor.getName().equals("Charlie")) {
              // add the invalid LP line
              lps.add(sensor.randomPoint().toLPBroken());
            } else {
              lps.add(sensor.randomPoint().toLP());
            }
          }
          // borrow a client from the pool
          InfluxDBClient client = clientPool.borrowClient();
          try {
            logger.info("[writeErrorRecover " + count + "] Writing " + lps.size()
              + " lps with client " + client.hashCode());
            client.writeRecords(lps);
          } catch (InfluxDBApiException ie) {
            logger.warning("[writeErrorRecover " + count + "] Write Error " + ie.getMessage());
          } finally {
            // make sure the client is returned to the pool even after an error
            clientPool.returnClient(client);
          }
          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
          count++;
        }
        logger.info(" [writeErrorRecover] shutting down");
      };

      // an error free query thread
      final Runnable queryOK = () -> {
        // delay start by 4 seconds
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(4));
        int count = 0;
        while (!shutdownAll.get()) {
          // borrow a client from the pool
          InfluxDBClient client = clientPool.borrowClient();

          // initiate the query and process the results
          try (Stream<PointValues> pvs = client.queryPoints(query)) {
            logger.info("[queryOK " + count + "] with client " + client.hashCode()
              + ": query returned " + pvs.toArray().length + " records");
          } catch (Exception e) {
            logger.severe("[queryOK " + count + "] unexpected query Error " + e.getMessage());
          } finally {
            // ensure the client is returned to the pool
            clientPool.returnClient(client);
          }
          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
          count++;
        }
        logger.info(" [queryOK] shutting down");
      };

      // an error-prone query thread
      final Runnable queryErrorRecover = () -> {
        // delay start by 6 seconds
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(6));
        int count = 0;
        while (!shutdownAll.get()) {
          // borrow a client from the pool
          InfluxDBClient client = clientPool.borrowClient();
          // every third query attempt results in an error
          String effectiveQuery = count > 0 && count % 3 == 0 ? badQuery : query;

          // attempt to execute the query and process the results
          try (Stream<PointValues> pvs = client.queryPoints(effectiveQuery)) {
            logger.info("[queryErrorRecover " + count + "] with client " + client.hashCode()
              + ": query returned " + pvs.toArray().length + " records");
          } catch (Exception e) {
            logger.warning("[queryErrorRecover " + count + "] with client " + client.hashCode()
              + ": query Error " + e.getMessage());
          } finally {
            // ensure the client is returned to the pool even after an error
            clientPool.returnClient(client);
          }
          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
          count++;
        }
        logger.info(" [queryErrorRecover] shutting down");
      };

      // control how long the example runs
      final Runnable timer = () -> {
        LockSupport.parkNanos(TimeUnit.MINUTES.toNanos(runTime));
        shutdownAll.set(true);
        logger.info(" [timer] Shutting down");
        logger.info("clientPool clients: active "
          + clientPool.activeCount()
          + " idle: " + clientPool.idleCount());
        executors.shutdown();
      };

      // trigger all threads
      executors.execute(writeOK);
      executors.execute(writeErrorRecover);
      executors.execute(queryOK);
      executors.execute(queryErrorRecover);
      executors.execute(timer);

      // ensure termination
      boolean returned;
      try {
        returned = executors.awaitTermination(runTime + 1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      logger.info("executors terminated cleanly: " + returned);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
