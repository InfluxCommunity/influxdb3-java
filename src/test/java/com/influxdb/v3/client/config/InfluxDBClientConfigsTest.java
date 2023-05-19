package com.influxdb.v3.client.config;

import com.influxdb.v3.client.write.WritePrecision;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class InfluxDBClientConfigsTest {

    private InfluxDBClientConfigs.Builder configsBuilder = new InfluxDBClientConfigs.Builder()
            .hostUrl("http://localhost:9999")
            .authToken("my-token")
            .organization("my-org")
            .database("my-db")
            .writePrecision(WritePrecision.NS)
            .responseTimeout(Duration.ofSeconds(30))
            .allowHttpRedirects(true)
            .disableServerCertificateValidation(true);

    @Test
    void equal() {
        InfluxDBClientConfigs configs = configsBuilder.build();

        Assertions.assertThat(configs).isEqualTo(configsBuilder.build());
        Assertions.assertThat(configs).isNotEqualTo(configsBuilder.database("database").build());
    }

    @Test
    void hash() {
        InfluxDBClientConfigs configs = configsBuilder.build();

        Assertions.assertThat(configs.hashCode()).isEqualTo(configsBuilder.build().hashCode());
        Assertions.assertThat(configs.hashCode()).isNotEqualTo(configsBuilder.database("database").build().hashCode());
    }
}
