package com.kafka.gridu.sofiya.streamsservice.config;

import com.kafka.gridu.sofiya.streamsservice.dto.Commit;
import com.kafka.gridu.sofiya.streamsservice.service.SchedulerService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Properties;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaStreamsConfigTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Commit> inputTopic;
    private SchedulerService schedulerServiceMock;

    @BeforeEach
    void setUp() {
        schedulerServiceMock = mock(SchedulerService.class);
        KafkaStreamsConfig config = new KafkaStreamsConfig(schedulerServiceMock);

        StreamsBuilder builder = new StreamsBuilder();
        config.kStream(builder);

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-analysis-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        String tempDir = System.getProperty("java.io.tmpdir") + "/kafka-streams-test-" + java.util.UUID.randomUUID();
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, tempDir);

        testDriver = new TopologyTestDriver(builder.build(), props);

        JsonSerde<Commit> commitSerde = new JsonSerde<>(Commit.class);
        commitSerde.deserializer().addTrustedPackages("*");
        commitSerde.deserializer().setUseTypeHeaders(false);

        inputTopic = testDriver.createInputTopic(
                "github_commits",
                Serdes.String().serializer(),
                commitSerde.serializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void shouldVerifyAllMetricsStreams() {
        Date friday = asDate(LocalDateTime.of(2024, 5, 17, 12, 0));

        Date saturday = asDate(LocalDateTime.of(2024, 5, 18, 12, 0));

        Commit c1 = new Commit("user1", "sha1", "short", friday, "Java", "repo1");
        Commit c2 = new Commit("user1", "sha2", "very long message", friday, "Python", "repo1");
        Commit c3 = new Commit("user2", "sha3", "weekend fix", saturday, "Java", "repo2");

        inputTopic.pipeInput("user1", c1);
        inputTopic.pipeInput("user1", c2);
        inputTopic.pipeInput("user2", c3);

        verify(schedulerServiceMock, atLeastOnce()).updateTotalCommits(3L);

        verify(schedulerServiceMock, atLeastOnce()).updateTotalContributorsCount("user1", 2L);
        verify(schedulerServiceMock, atLeastOnce()).updateTotalContributorsCount("user2", 1L);

        verify(schedulerServiceMock, atLeastOnce()).updateLanguageStatsCount("Java", 2L);
        verify(schedulerServiceMock, atLeastOnce()).updateLanguageStatsCount("Python", 1L);

        verify(schedulerServiceMock, atLeastOnce()).updateWeekendCommits(1L);

        verify(schedulerServiceMock, atLeastOnce()).updateMostActiveDate("FRIDAY", 2L);
        verify(schedulerServiceMock, atLeastOnce()).updateMostActiveDate("SATURDAY", 1L);

        verify(schedulerServiceMock, atLeastOnce()).updateTotalRepos(2L);

        verify(schedulerServiceMock, atLeastOnce()).updateAvgMessageLength(11L);
    }

    private Date asDate(LocalDateTime ldt) {
        return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    }
}
