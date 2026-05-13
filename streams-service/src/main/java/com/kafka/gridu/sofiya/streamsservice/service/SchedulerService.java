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
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@EnableScheduling
public class SchedulerService {

    private final AtomicLong totalCommits = new AtomicLong(0);
    private final AtomicLong avgMessageLength = new AtomicLong(0);
    private final AtomicLong weekendCommits = new AtomicLong(0);
    private final AtomicLong totalRepos = new AtomicLong(0);

    private final Map<String, Long> mostActiveDate = new ConcurrentHashMap<>();
    private final Map<String, Long> userCommits = new ConcurrentHashMap<>();
    private final Map<String, Long> languageStats  = new ConcurrentHashMap<>();


    public void updateTotalCommits(long count) {
        totalCommits.set(count);
    }

    public void updateAvgMessageLength(long count) {
        avgMessageLength.set(count);
    }

    public void updateWeekendCommits(long count) {
        weekendCommits.set(count);
    }

    public void updateTotalRepos(long count) {
        totalRepos.set(count);
    }

    public void updateMostActiveDate(String day, long count) {
        mostActiveDate.put(day, count);
    }

    public void updateUserCommitsCount(String user, long count) {
        userCommits.put(user, count);
    }

    public void updateLanguageStatsCount(String lang, long count) {
        languageStats.put(lang, count);
    }

    @Scheduled(fixedRate = 10000)
    public void exportMetricsToFile() {
        try {
            Function<Map.Entry<String, Long>, String> entryListCommitsFunction =
                    e -> e.getKey() + ": " + e.getValue() + " commits";
            String topContributors = userCommits.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(5)
                    .map(entryListCommitsFunction)
                    .collect(Collectors.joining("\n"));

            String langStats = languageStats.entrySet().stream()
                    .map(entryListCommitsFunction)
                    .collect(Collectors.joining("\n"));

            String mostActiveDateStats = mostActiveDate.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .map(entryListCommitsFunction)
                    .collect(Collectors.joining("\n"));

            String content = String.format(
                    """
                    - Total Commits: %d
                    - Total Contributors: %d
                    - Top 5 Contributors:
                    %s
                    - Top Languages:
                    %s
                    CUSTOM METRICS:
                    - Avg Message Length: %d characters
                    - Weekend Activity: %d commits
                    - Most Active Day:
                    %s
                    - Total Repositories Scanned: %d
                    """
                    ,
                    totalCommits.get(),
                    userCommits.size(),
                    topContributors.isEmpty() ? "No data yet" : topContributors,
                    langStats.isEmpty() ? "No data yet" : langStats,
                    avgMessageLength.get(),
                    weekendCommits.get(),
                    mostActiveDateStats.isEmpty() ? "No data yet" : mostActiveDateStats,
                    totalRepos.get());

            Files.writeString(Path.of("metrics.txt"), content);
            log.info("Metrics successfully updated in metrics.txt");

        } catch (Exception e) {
            log.error("Failed to write metrics to file", e);
        }
    }
}