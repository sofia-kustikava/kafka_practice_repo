package com.kafka.gridu.sofiya.streamsservice.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class SchedulerServiceTest {
    private SchedulerService schedulerService;

    @BeforeEach
    void setUp() {
        schedulerService = new SchedulerService();
    }

    @Test
    void shouldCorrectlyFormatMetrics() throws IOException {
        schedulerService.updateTotalCommits(100);
        schedulerService.updateTotalContributorsCount("user1", 10);
        schedulerService.updateTotalContributorsCount("user2", 50);
        schedulerService.updateLanguageStatsCount("Java", 5);

        schedulerService.exportMetricsToFile();

        Path path = Path.of("metrics.txt");
        assertThat(Files.exists(path)).isTrue();

        String content = Files.readString(path);
        assertThat(content).contains("Total Commits: 100");
        assertThat(content).contains("user2: 50 commits");
        assertThat(content).contains("Java: 5 commits");
    }
}
