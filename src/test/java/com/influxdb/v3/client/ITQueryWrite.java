package com.influxdb.v3.client;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;

@SuppressWarnings("resource")
class ITQueryWrite {

    private static final Logger LOG = LoggerFactory.getLogger(ITQueryWrite.class);

    private static final List<GenericContainer<?>> DOCKER_CONTAINERS = Arrays.asList(
            System.getenv("FLIGHT_SQL_URL") == null
                    ?
                    new GenericContainer<>("voltrondata/flight-sql:arrow-11.0.0")
                            .withExposedPorts(31337)
                            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(31337), new ExposedPort(31337)))))
                            .withEnv("FLIGHT_PASSWORD", "flight_password")
                            .withEnv("PRINT_QUERIES", "1")
                    :
                    null,
            System.getenv("INFLUXDB_URL") == null
                    ?
                    new GenericContainer<>("influxdb:latest")
                            .withExposedPorts(8086, 8086)
                            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(8086), new ExposedPort(8086)))))
                            .withEnv("DOCKER_INFLUXDB_INIT_MODE", "setup")
                            .withEnv("DOCKER_INFLUXDB_INIT_USERNAME", "my-user")
                            .withEnv("DOCKER_INFLUXDB_INIT_PASSWORD", "my-password")
                            .withEnv("DOCKER_INFLUXDB_INIT_ORG", "my-org")
                            .withEnv("DOCKER_INFLUXDB_INIT_BUCKET", "my-bucket")
                            .withEnv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "my-token")
                    :
                    null
    );

    private InfluxDBClientImpl client;

    @AfterEach
    void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @BeforeAll
    public static void startContainers() throws InterruptedException {
        for (GenericContainer<?> genericContainer : DOCKER_CONTAINERS) {
            if (genericContainer != null) {
                genericContainer.start();
            }
        }

        Thread.sleep(5_000);
    }

    @AfterAll
    public static void stopContainers() {
        for (GenericContainer<?> genericContainer : DOCKER_CONTAINERS) {
            if (genericContainer != null) {
                genericContainer.stop();
            }
        }
    }

    @Test
    void query() {
        client = initFlightSql();
        try (Stream<Object[]> rows = client.query("SELECT * FROM nation")) {

            rows.forEach(row -> {

                Object nation = row[1];

                Assertions.assertThat(row[0]).isInstanceOf(Number.class);
                if (row[0].equals(0)) {
                    Assertions.assertThat(nation).isEqualTo(new Text("ALGERIA"));
                }
                if (row[0].equals(24)) {
                    Assertions.assertThat(nation).isEqualTo(new Text("UNITED STATES"));
                }

                LOG.info(nation.toString());
            });
        }
    }

    @Test
    void queryBatches() {
        client = initFlightSql();

        try (Stream<VectorSchemaRoot> batches = client.queryBatches("SELECT * FROM nation")) {

            List<VectorSchemaRoot> batchesAsList = batches.collect(Collectors.toList());

            Assertions.assertThat(batchesAsList).hasSize(1);
        }
    }

    private InfluxDBClientImpl initFlightSql() {

        String hostUrl = System.getenv("FLIGHT_SQL_URL");

        HashMap<String, String> headers = new HashMap<String, String>() {{
            put("Authorization", "Basic " + Base64.getEncoder().encodeToString(("flight_username:flight_password").getBytes()));
        }};

        return new InfluxDBClientImpl(new InfluxDBClientConfigs.Builder()
                .hostUrl(hostUrl != null ? hostUrl : "https://localhost:31337")
                .database("database")
                .disableServerCertificateValidation(true)
                .build(), headers);
    }
}
