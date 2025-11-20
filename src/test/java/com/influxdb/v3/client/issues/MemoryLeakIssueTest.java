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
package com.influxdb.v3.client.issues;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

public class MemoryLeakIssueTest {

    private static final Logger LOG = Logger.getLogger(MemoryLeakIssueTest.class.getName());

    /**
     * Tests that interrupting a thread during stream consumption does not cause Arrow memory leaks.
     * <p>
     * This test creates a query thread that slowly consumes results, then interrupts it mid-processing.
     * The interrupt causes FlightStream.close() to throw InterruptedException, which previously bypassed
     * cleanup code and left Apache Arrow buffers unreleased. With the fix, client.close() should complete
     * successfully without throwing "Memory was leaked" errors.
     */
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void testStreamCloseWithThreadInterrupt() throws Exception {
        String host = System.getenv("TESTING_INFLUXDB_URL");
        String token = System.getenv("TESTING_INFLUXDB_TOKEN");
        String database = System.getenv("TESTING_INFLUXDB_DATABASE");
        String measurement = "memory_leak_test_" + System.currentTimeMillis();
        String sql = String.format("SELECT * FROM %s", measurement);

        // Prepare config
        ClientConfig config = new ClientConfig.Builder()
                .host(host)
                .token(token.toCharArray())
                .database(database)
                .writeNoSync(true)
                .build();

        try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
            // Write test data
            LOG.info("Writing test data...");
            for (int i = 0; i < 3; i++) {
                client.writeRecord(String.format("%s,id=%04d temp=%f",
                        measurement, i, 20.0 + Math.random() * 10));
            }

            // Wait for data to be queryable (CI environments can be slower)
            LOG.info("Waiting for data to be available...");
            int attempts = 0;
            boolean hasData = false;
            while (attempts < 10 && !hasData) {
                try (Stream<PointValues> testStream = client.queryPoints(sql)) {
                    hasData = testStream.findFirst().isPresent();
                }
                if (!hasData) {
                    LOG.info("Data not yet available, waiting... (attempt " + (attempts + 1) + "/10)");
                    TimeUnit.MILLISECONDS.sleep(500);
                    attempts++;
                }
            }

            if (!hasData) {
                Assertions.fail("No data available after writing and waiting " + (attempts * 500) + "ms");
            }
            LOG.info("Data is available, starting test...");

        }

        // Query data
        InfluxDBClient client = InfluxDBClient.getInstance(config);
        //noinspection TryFinallyCanBeTryWithResources
        try {
            // Synchronization to ensure we interrupt during consumption
            CountDownLatch consumingStarted = new CountDownLatch(1);
            AtomicInteger rowsProcessed = new AtomicInteger(0);
            AtomicInteger exceptionsThrown = new AtomicInteger(0);

            Thread queryThread = new Thread(() -> {
                try (Stream<PointValues> stream = client.queryPoints(sql)) {
                    LOG.info("queryPoints returned");
                    stream.forEach(pv -> {
                        int count = rowsProcessed.incrementAndGet();

                        // Signal that we've started consuming
                        if (count == 1) {
                            LOG.info("Started consuming - ready for interrupt");
                            consumingStarted.countDown();
                        }

                        try {
                            // Slow consumption to ensure we're mid-stream when interrupted
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            LOG.info("INTERRUPTED during consume! (after " + count + " rows)");
                            Thread.currentThread().interrupt();
                            // Throw exception to stop stream consumption immediately; try-with-resources will then
                            // close stream in interrupted state
                            throw new RuntimeException("Interrupted", e);
                        }
                    });
                } catch (Exception e) {
                    exceptionsThrown.incrementAndGet();
                    LOG.info("Exception caught: " + e);
                }
            });

            queryThread.start();

            // Wait for thread to start consuming
            if (!consumingStarted.await(10, TimeUnit.SECONDS)) {
                Assertions.fail("Thread didn't start consuming in time!");
            }

            // Give it a moment to be mid-processing
            TimeUnit.MILLISECONDS.sleep(50);

            // Interrupt during processing
            LOG.info("Interrupting thread...");
            queryThread.interrupt();

            // Wait for thread to finish
            queryThread.join(10000);

            // Verify that thread started processing rows
            if (rowsProcessed.get() == 0) {
                Assertions.fail("Thread didn't process any rows");
            }

            // Verify that exception was thrown due to interrupt
            if (exceptionsThrown.get() == 0) {
                Assertions.fail("No exception was thrown - interrupt might not have worked");
            }

        } catch (Exception e) {
            LOG.severe("Test failed: " + e.getMessage());
            throw e;
        } finally {
            // Now close the client.
            // It should not throw `Memory was leaked by query. Memory leaked: (...)` error!
            LOG.info("Closing the client...");
            client.close();
        }
    }
}
