package com.kafka.gridu.sofiya.githubproducerservice.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class GithubServiceTest {
    private GithubService githubService;

    @BeforeEach
    void setUp() {
        githubService = new GithubService();
    }

    @ParameterizedTest(name = "Interval {0} should count the time correctly")
    @CsvSource({
            "2d, 2, DAYS",
            "3h, 3, HOURS",
            "1w, 7, DAYS",
            "1y, 365, DAYS"
    })
    @DisplayName("Valid interval parsing")
    void shouldParseIntervalCorrectly(String interval, int amount, ChronoUnit unit) {
        LocalDateTime now = LocalDateTime.now();
        Date expectedDate = Date.from(now.minus(amount, unit)
                .atZone(ZoneId.systemDefault())
                .toInstant());

        Date actualDate = ReflectionTestUtils.invokeMethod(githubService, "parseInterval", interval);

        assertThat(actualDate).isCloseTo(expectedDate, 2000);
    }

    @ParameterizedTest(name = "Interval {0} should be invalid")
    @ValueSource(strings = {
            "invalid",
            "hhhhhhhhh",
            "3yyy",
            "",
            "2234"
    })
    @DisplayName("Wrong input intervals parsing")
    void shouldReturnDefaultIntervalForInvalidInput(String invalidInterval) {
        Date actualDate = ReflectionTestUtils.invokeMethod(githubService, "parseInterval", invalidInterval);
        Date expectedDate = Date.from(LocalDateTime.now().minusDays(1)
                .atZone(ZoneId.systemDefault()).toInstant());

        assertThat(actualDate).isCloseTo(expectedDate, 2000);
    }
}
