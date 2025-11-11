package org.influxdb.v3;

import java.time.Duration;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.influxdb.v3.config.AppConfig;
import org.influxdb.v3.reading.EnvReading;
import org.influxdb.v3.sensor.SensorCollection;
import org.influxdb.v3.service.PersistService;
import org.influxdb.v3.service.ReadingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.influxdb.v3.client.PointValues;

@Component
public final class Application {

    private Application() { }

    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) {
        try (var ctx = new AnnotationConfigApplicationContext(AppConfig.class)) {
            ctx.registerShutdownHook();

            SensorCollection sensors = ctx.getBean(SensorCollection.class);
            PersistService persistService = ctx.getBean(PersistService.class);
            persistService.persistDataRandom(sensors, 1, Duration.ofMinutes(5));

            ReadingsService readingsService = ctx.getBean(ReadingsService.class);
            logger.info("==== [ Get as Point Values ] ====");
            /*
               Be sure to use streams in try-with-resources blocks, failure to do so
               may not close the underlying Arrow FlightStream properly, and
               a memory leak can result.
             */
            try (Stream<PointValues> stream = readingsService.getAllReadingsAsPV()) {
                logPVStream(stream);
            }
            logger.info("==== [ Get as Mapped EnvReadings ] ====");
            try (Stream<EnvReading> stream = readingsService.getAllReadings()) {
                logEnvReadingStream(stream);
            }
            logger.info("==== [ Get as Object Array ] ====");
            try (Stream<Object[]> stream = readingsService.getAllReadingsAsObj()) {
                logObjArrayStream(stream);
            }
        }
    }

    public static void logPVStream(final @Nonnull Stream<PointValues> pvs) {
        StringBuilder pvResults = new StringBuilder();
        pvs.forEach(pv -> {
            pvResults.append(String.format("%s, ", pv.getTimestamp()));
            pvResults.append(String.format("name: %s, ", pv.getTag(("name"))));
            pvResults.append(String.format("model: %s, ", pv.getTag(("model"))));
            pvResults.append(String.format("id: %s, ", pv.getTag(("id"))));
            pvResults.append(String.format("temp: %3.2f ", pv.getFloatField(("temp"))));
            pvResults.append(String.format("press: %3.2f ", pv.getFloatField(("press"))));
            pvResults.append(String.format("humid: %3.2f ", pv.getFloatField(("humid"))));
            pvResults.append("\n");
        });
        logger.info("PointValueResults:\n{}\n", pvResults);
    }

    public static void logObjArrayStream(final @Nonnull Stream<Object[]> oas) {
        StringBuilder results = new StringBuilder();
        oas.forEach(o -> {
            for (int i = 0; i < o.length; i++) {
                results.append(String.format("%s, ", o[i]));
            }
            results.append("\n");
        });
        logger.info("ObjectArrayStream results:\n{}\n", results);
    }

    public static void logEnvReadingStream(final @Nonnull Stream<EnvReading> evs) {
        StringBuilder result = new StringBuilder();
        evs.forEach(point -> {
            result.append(String.format("%s\n", point.toString()));
        });
        logger.info(result.toString());
    }
}