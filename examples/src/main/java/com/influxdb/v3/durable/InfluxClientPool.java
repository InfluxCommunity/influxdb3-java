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
import java.util.Stack;
import java.util.logging.Logger;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.ClientConfig;

/**
 * And example pool for InfluxDBClient clients.
 * <p>
 * All clients handled by the pool will share the same basic configuration.
 */
public class InfluxClientPool implements AutoCloseable {

  Logger logger = Logger.getLogger(InfluxClientPool.class.getName());

  private static final int DEFAULT_MAX_SIZE = 4;

  // Container for clients waiting to be used.
  Stack<InfluxDBClient> idlers = new Stack<>();
  // Container for clients currently in use.
  List<InfluxDBClient> runners = new ArrayList<>();

  int maxSize;

  // The shared configuration.
  final ClientConfig clientConfig;

  /**
   * Basic Constructor, uses DEFAULT_MAX_SIZE for maxSize.
   * <p>
   * @param clientConfig - the standard configuration for all clients managed by the pool.
   */
  public InfluxClientPool(final ClientConfig clientConfig) {
    this(clientConfig, DEFAULT_MAX_SIZE);
  }

  /**
   * Basic constructor.
   * <p>
   * @param clientConfig - the standard configuration for all clients managed by the pool.
   */
  public InfluxClientPool(final ClientConfig clientConfig, final int maxSize) {
    this.clientConfig = clientConfig;
    this.maxSize = maxSize;
  }

  /**
   * Checks for a free client in the idle stack.  If the idle stack
   * is empty, it generates a new client.
   *
   * @return - An InfluxDBClient ready for use.
   */
  public synchronized InfluxDBClient borrowClient() {
    InfluxDBClient client;
    if (idlers.isEmpty()) {
      client = InfluxDBClient.getInstance(clientConfig);
      runners.add(client);
      if (activeCount() >= maxSize) {
        logger.severe("Max pool size " + maxSize + " exceeded: " + "actives "
          + activeCount() + " idles " + idleCount()
          + " (hint: Is there a process hogging zombie clients?)");
      }
    } else {
      client = idlers.pop();
      runners.add(client);
    }
    logger.info("Lending client " + client.hashCode());
    return client;
  }

  /**
   * Invalidate a client if some unwanted exception state is encountered,
   * or if for some other reason it is unusable or no longer needed.
   *
   * @param client - client to be closed and flagged for garbage collection.
   */
  public synchronized void invalidateClient(final InfluxDBClient client) {
    runners.remove(client);
    try {
      client.close();
    } catch (Exception e) {
      logger.warning("Exception occurred when invalidating client "
        + client.hashCode() + ": " + e.getMessage());
    }
  }

  /**
   * Return the client to the idle stack within the pool
   * when it is no longer needed but can still be reused.
   *
   * @param client - the client to be returned.
   */
  public synchronized void returnClient(final InfluxDBClient client) {
    logger.info("Returning client " + client.hashCode());
    runners.remove(client);
    idlers.push(client);
  }

  /**
   * Handle closing all resources.
   * <p>
   * First return active clients to the idle stack.
   * Then close all clients in the idle stack.
   *
   * @throws Exception
   */
  @Override
  public synchronized void close() throws Exception {
    logger.info("Closing client pool");
    int stillActiveCount = activeCount();
    for (int i = stillActiveCount - 1; i >= 0; i--) {
      returnClient(runners.get(i));
    }
    while (!idlers.isEmpty()) {
      try (InfluxDBClient client = idlers.pop()) {
        logger.info("Closing client " + client.hashCode());
      } catch (IllegalStateException e) {
        StringBuilder msg = new StringBuilder("IllegalStateException when closing client. ");
        msg.append(e.getMessage());
        if (e.getMessage().contains("leaked")) {
          msg.append(" (hint: were all streams returned from queries closed correctly?)");
        }
        logger.warning(msg.toString());
      } catch (Exception e) { // client close should be automatic
        logger.warning("Exception when closing client " + e.getMessage());
      }
    }
  }

  public int activeCount() {
    return runners.size();
  }

  public int idleCount() {
    return idlers.size();
  }
}
