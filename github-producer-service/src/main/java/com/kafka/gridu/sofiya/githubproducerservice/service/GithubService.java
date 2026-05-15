package com.kafka.gridu.sofiya.githubproducerservice.service;

import com.kafka.gridu.sofiya.githubproducerservice.dto.GithubAccount;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Getter
@Service
@RequiredArgsConstructor
public class GithubService {

    @Value("${github.token}")
    private String token;

    private GitHub github;

    @PostConstruct
    public void init() throws IOException {
        this.github = new GitHubBuilder()
                .withOAuthToken(token)
                .build();
        log.info("Connected to GitHub as: {}", github.getMyself().getLogin());
    }

    public List<GHCommit> fetchCommits(GithubAccount githubAccount) {
        try {
            String username = githubAccount.username();
            Date since = parseInterval(githubAccount.interval());

            log.info("Fetching commits for user: {} since {}", username, since);

            return github.getUser(username).listRepositories().toList().stream()
                    .flatMap(repo -> {
                        try {
                            return repo.queryCommits()
                                    .author(username)
                                    .since(since)
                                    .list()
                                    .toList()
                                    .stream();
                        } catch (Exception e) {
                            log.error("Error fetching commits for repo: {}", repo.getName());
                            return Stream.empty();
                        }
                    })
                    .toList();
        } catch (Exception e) {
            log.error("Failed to fetch data from GitHub", e);
            return Collections.emptyList();
        }
    }

    private Date parseInterval(String interval) {
        LocalDateTime now = LocalDateTime.now();
        if (interval != null && interval.matches("^\\d+[dhwy]$")) {
            char unit = interval.charAt(interval.length() - 1);
            int value = Integer.parseInt(interval.substring(0, interval.length() - 1));

            return switch (unit) {
                case 'd' -> Date.from(now.minusDays(value).atZone(ZoneId.systemDefault()).toInstant());
                case 'h' -> Date.from(now.minusHours(value).atZone(ZoneId.systemDefault()).toInstant());
                case 'w' -> Date.from(now.minusWeeks(value).atZone(ZoneId.systemDefault()).toInstant());
                case 'y' -> Date.from(now.minusYears(value).atZone(ZoneId.systemDefault()).toInstant());
                default -> Date.from(now.minusDays(1).atZone(ZoneId.systemDefault()).toInstant());
            };
        }
        log.warn("Invalid interval format: {}. Using default (1d).", interval);
        return Date.from(now.minusDays(1).atZone(ZoneId.systemDefault()).toInstant());
    }
}
