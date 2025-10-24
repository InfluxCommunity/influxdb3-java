package org.influxdb.v3.service;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;
import org.apache.arrow.flight.FlightRuntimeException;
import org.influxdb.v3.reading.EnvReading;
import org.influxdb.v3.sensor.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;


@Service
public class ReadingsService {

    Logger logger = LoggerFactory.getLogger(ReadingsService.class);

    @Value("${readings.query1}")
    String query1;

    InfluxDBClient influxDBClientBase;

    @Autowired
    public ReadingsService(@Qualifier("influxDBClient")InfluxDBClient influxDBClient) {
        logger.debug("instantiating ReadingsService");
        this.influxDBClientBase = influxDBClient;
    }

    //@Retryable(retryFor = ConnectException.class,
    //    maxAttemptsExpression = "${query.retry.maxAttempts}",
    //    backoff = @Backoff(delayExpression = "${query.retry.maxDelay}"))
    public Stream<EnvReading> getAllReadings() {
        RetryTemplate retryTemplate = RetryTemplate.builder()
            .retryOn(List.of(InfluxDBApiException.class,
                FlightRuntimeException.class,
                ConnectException.class))
            .maxAttempts(10)
            .exponentialBackoff(Duration.ofMillis(100), 2, Duration.ofSeconds(10))
            .build();

        return retryTemplate.execute(context -> {
            logger.info("ReadingsService getting all readings");
            logger.info("RetryContext {}", context);
            return buildEnvReadings(influxDBClientBase.queryPoints(query1));
        });
    }

    public Stream<PointValues> getAllReadingsAsPV(){
        /* RetryTemplate retryTemplate = RetryTemplate.builder()
            .retryOn(List.of(InfluxDBApiException.class,
                FlightRuntimeException.class,
                ConnectException.class))
            .maxAttempts(10)
            .exponentialBackoff(Duration.ofMillis(100), 2, Duration.ofSeconds(10))
            .build(); */

        //return retryTemplate.execute(context -> {
          //  logger.info("ReadingsService getting all readings");
          //  logger.info("RetryContext {}", context);
            return influxDBClientBase.queryPoints(query1);
        // });

    }

    public Stream<Object[]> getAllReadingsAsObj(){
        return influxDBClientBase.query(query1);
    }

    @Recover
    public void recoverInflux(InfluxDBApiException influxDBApiException) {
        logger.info("ReadingsService recovering {}", influxDBApiException.getMessage());
    }

    @Recover
    public void recoverFlight(FlightRuntimeException flightRuntimeException) {
        logger.info("ReadingsService recoverFlight {}", flightRuntimeException.getMessage());
    }

    @Recover void recoverConnect(ConnectException connectException) {
        logger.info("ReadingsService recoverConnect {}", connectException.getMessage());
    }

    Stream<EnvReading> buildEnvReadings(Stream<PointValues> pointValuesStream) {
        return pointValuesStream.map(e -> {
            double temp = e.getFloatField("temp") == null ? 0 : e.getFloatField("temp");
            double humid = e.getFloatField("humid") == null ? 0 : e.getFloatField("humid");
            double press = e.getFloatField("press") == null ? 0 : e.getFloatField("press");
            Number timestamp = e.getTimestamp() == null ? 0 : e.getTimestamp();
            return new EnvReading(new Sensor(e.getTag("name"), e.getTag("model"), e.getTag("id")),
                temp, humid, press, parseTimestamp(timestamp));
        });
    }

    private static Instant parseTimestamp(@Nonnull Number timestamp){
        return Instant.ofEpochMilli(timestamp.longValue() / 1_000_000);
    }
}
