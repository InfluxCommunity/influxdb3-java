package org.influxdb.v3.config;

import com.influxdb.v3.client.InfluxDBClient;
import org.influxdb.v3.sensor.Sensor;
import org.influxdb.v3.sensor.SensorCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;

@Configuration
@EnableRetry
@ComponentScan(basePackages = "org.influxdb.v3")
@PropertySource("classpath:application.properties")
public class AppConfig {

    Logger logger = LoggerFactory.getLogger(AppConfig.class);

    static List<Sensor> sensors = List.of(new Sensor("Able", "R2D2", "123"),
            new Sensor("Baker", "C3PO", "456"),
            new Sensor("Charle", "Robbie", "789"),
            new Sensor("Delta", "R2D2", "abc"),
            new Sensor( "Easy", "C3PO", "def"
        )
    );

    @Value("${influxdb.url}")
    private String influxDBUrl;

    @Value("${influxdb.token}")
    private String influxDBToken;

    @Value("${influxdb.database}")
    private String influxDBDatabase;

    @Bean(name={"internalSensors"})
    public SensorCollection sensorCollectionInit(){
        logger.debug("sensorCollection");
        return new SensorCollection(sensors);
    }

    @Bean
    @Qualifier("influxDBClient")
    public InfluxDBClient influxDBClient(){
        logger.debug("influxDBClientBaseInit with " +  influxDBUrl);
        return InfluxDBClient.getInstance(influxDBUrl, influxDBToken.toCharArray(), influxDBDatabase);
    }

    @Bean
    public RetryTemplate retryTemplate(){
        RetryTemplate retryTemplate = new RetryTemplate();

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(2000l);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(2);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}
