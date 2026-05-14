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
        if (interval.endsWith("d")) {
            int days = Integer.parseInt(interval.replace("d", ""));
            return Date.from(now.minusDays(days).atZone(ZoneId.systemDefault()).toInstant());
        } else if (interval.endsWith("h")) {
            int hours = Integer.parseInt(interval.replace("h", ""));
            return Date.from(now.minusHours(hours).atZone(ZoneId.systemDefault()).toInstant());
        } else if (interval.endsWith("w")) {
            int weeks = Integer.parseInt(interval.replace("w", ""));
            return Date.from(now.minusWeeks(weeks).atZone(ZoneId.systemDefault()).toInstant());
        } else if (interval.endsWith("y")) {
            int years = Integer.parseInt(interval.replace("y", ""));
            return Date.from(now.minusYears(years).atZone(ZoneId.systemDefault()).toInstant());
        }
        return Date.from(now.minusDays(1).atZone(ZoneId.systemDefault()).toInstant());
    }
}
