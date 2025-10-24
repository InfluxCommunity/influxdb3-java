package org.influxdb.v3.service;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import org.apache.arrow.flight.FlightRuntimeException;
import org.influxdb.v3.reading.RandomEnvReading;
import org.influxdb.v3.sensor.Sensor;
import org.influxdb.v3.sensor.SensorCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class PersistService {

    Logger logger = LoggerFactory.getLogger(PersistService.class);

    InfluxDBClient influxDBClientBase;

    @Value("${influxdb.measurement}")
    String measurement;

    @Value("${app.debug}")
    boolean debug;

    @Autowired
    public PersistService(@Qualifier("influxDBClient")InfluxDBClient influxDBClient) {
        this.influxDBClientBase = influxDBClient;
    }

    //@Retryable(retryFor = {InfluxDBApiException.class},
    //    maxAttemptsExpression = "${write.retry.maxAttempts}",
    //    backoff = @Backoff(delayExpression = "${write.retry.maxDelay}")
    //)
    // @Retryable()
    public void persistDataRandom(SensorCollection sensors, int count, Duration interval) {
        logger.info("persistDataRandom " + count + " sensor sets at interval: " + interval);

        RetryTemplate retryTemplate = RetryTemplate.builder()
            .maxAttempts(10)
            .exponentialBackoff(100, 2, 10000)
            .retryOn(List.of(InfluxDBApiException.class, FlightRuntimeException.class, ConnectException.class))
            .traversingCauses()
            .build();

        retryTemplate.execute(context -> {
            logger.info("DEBUG conext {}", context);
                Instant current = Instant.now().minus(Duration.ofMillis(count * interval.toMillis()));
                Instant end = Instant.now();
                int current_count = 0;
                while (current_count < count) {
                    List<Point> points = new ArrayList<>();
                    for (Sensor sensor : sensors.getSensors()) {
                        Point reading = RandomEnvReading.genReading(sensor).toPoint(measurement, current);
                        points.add(reading);
                        logger.info("reading {}", reading.toLineProtocol());
                    }
                    influxDBClientBase.writePoints(points);
                    current = current.plus(interval);
                    current_count++;
                }
                return null;
        });
    }

    /* TODO
    Runnable randomDataGenerator(Instant start,
                                 Instant end,
                                 Duration pauseInterval) {
        if (end.isBefore(start)) {
            throw new IllegalArgumentException(String.format("end %s is before start %s", end, start));
        }
        return () -> {
            while(Instant.now().isBefore(start)) {
                logger.info("Waiting to start randomDataGenerator at {}", start);
                LockSupport.parkNanos(pauseInterval.toNanos());
            }
            while(Instant.now().isBefore(end)) {
                logger.info("Waiting to end randomDataGenerator at {}", end);
                LockSupport.parkNanos(pauseInterval.toNanos());
            }
            logger.info("RandomDataGenerator ending at {}", Instant.now());
        };
    } */

    @Recover
    public void recoverInflux(InfluxDBApiException influxDBApiException) {
        logger.info("PersistService recoverInflux {}", influxDBApiException.getMessage());
    }

    @Recover
    public void recoverFlight(FlightRuntimeException flightRuntimeException) {
        logger.info("PersistService recoverFlight {}", flightRuntimeException.getMessage());
    }

    @Recover void recoverConnect(ConnectException connectException) {
        logger.info("PersistService recoverConnect {}", connectException.getMessage());
    }
}
