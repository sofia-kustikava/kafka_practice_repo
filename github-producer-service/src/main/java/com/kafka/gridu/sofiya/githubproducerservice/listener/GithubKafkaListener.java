package com.kafka.gridu.sofiya.githubproducerservice.listener;

import com.kafka.gridu.sofiya.githubproducerservice.dto.Commit;
import com.kafka.gridu.sofiya.githubproducerservice.dto.GithubAccount;
import com.kafka.gridu.sofiya.githubproducerservice.service.GithubService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHCommit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class GithubKafkaListener {
    public static final String COMMITS_TOPIC = "github_commits";
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final GithubService githubService;

    @KafkaListener(topics = "github_accounts", groupId = "github-ingestion-group")
    public void listenAccounts(GithubAccount account) {
        log.info("Received account from Kafka: {}", account.username());
        try {
            List<GHCommit> commits = githubService.fetchCommits(account);
            log.info("Found {} commits for user: {}", commits.size(), account.username());
            for (GHCommit commit : commits) {
                String language = commit.getOwner().getLanguage();
                Commit commitRecord = new Commit(
                        account.username(),
                        commit.getSHA1(),
                        commit.getCommitShortInfo().getMessage(),
                        commit.getCommitDate(),
                        language != null ? language : "Unknown",
                        commit.getOwner().getName()
                );
                kafkaTemplate.send(COMMITS_TOPIC, commitRecord.username(), commitRecord);
            }
            log.info("All commits for user {} sent to topic {}", account.username(), COMMITS_TOPIC);
        } catch (Exception e) {
            log.error("Critical error while processing account {}", account.username(), e);
        }
    }
}
