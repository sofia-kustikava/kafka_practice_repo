package com.kafka.gridu.sofiya.streamsservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@EnableScheduling
public class SchedulerService {

    private final AtomicLong totalCommits = new AtomicLong(0);
    private final Map<String, Long> userCommits = new ConcurrentHashMap<>();

    public void updateTotal(long count) {
        totalCommits.set(count);
    }

    public void updateUserCount(String user, long count) {
        userCommits.put(user, count);
    }

    @Scheduled(fixedRate = 10000)
    public void exportMetricsToFile() {
        try {
            String top5 = userCommits.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(5).map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining("\n"));

            String content = String.format(
                    """
                        === GitHub Metrics ===
                        Total Commits: %d
                        Total Contributors: %d
                        --- Top 5 Contributors ---
                        %s
                    """
                    , totalCommits.get(), userCommits.size(), top5.isEmpty() ? "No data yet" : top5);

            Files.writeString(Path.of("metrics.txt"), content);
            log.info("Metrics successfully updated in metrics.txt");

        } catch (Exception e) {
            log.error("Failed to write metrics to file", e);
        }
    }
}