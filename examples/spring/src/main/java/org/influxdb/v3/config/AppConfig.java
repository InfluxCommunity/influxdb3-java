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
package org.influxdb.v3.config;

import java.net.ConnectException;
import java.util.List;

import org.apache.arrow.flight.FlightRuntimeException;
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
import org.springframework.retry.support.RetryTemplate;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBClient;

@Configuration
@EnableRetry
@ComponentScan(basePackages = "org.influxdb.v3")
@PropertySource("classpath:application.properties")
public class AppConfig {

    Logger logger = LoggerFactory.getLogger(AppConfig.class);

    static List<Sensor> sensors = List.of(new Sensor("Able", "R2D2", "123"),
            new Sensor("Baker", "C3PO", "456"),
            new Sensor("Charlie", "Robbie", "789"),
            new Sensor("Delta", "R2D2", "abc"),
            new Sensor("Easy", "C3PO", "def")
    );

    @Value("${influxdb.url}")
    private String influxDBUrl;

    @Value("${influxdb.token}")
    private String influxDBToken;

    @Value("${influxdb.database}")
    private String influxDBDatabase;

    @Bean(name = {"internalSensors"})
    public SensorCollection sensorCollectionInit() {
        logger.debug("sensorCollection");
        return new SensorCollection(sensors);
    }

    @Bean
    public InfluxDBClient influxDBClient() {
        logger.debug("influxDBClientBaseInit with " +  influxDBUrl);
        return InfluxDBClient.getInstance(influxDBUrl, influxDBToken.toCharArray(), influxDBDatabase);
    }

    @Bean
    @Qualifier("writesTemplate")
    public RetryTemplate retryTemplateWrites() {
        return RetryTemplate.builder()
            .maxAttempts(5)
            .exponentialBackoff(100, 2, 10000)
            .retryOn(List.of(InfluxDBApiException.class, ConnectException.class))
            .traversingCauses()
            .build();
    }

    @Bean
    @Qualifier("readsTemplate")
    public RetryTemplate retryTemplateReads() {
        return RetryTemplate.builder()
            .maxAttempts(3)
            .exponentialBackoff(100, 2, 10000)
            .retryOn(List.of(InfluxDBApiException.class, FlightRuntimeException.class, ConnectException.class))
            .traversingCauses()
            .build();
    }
}
