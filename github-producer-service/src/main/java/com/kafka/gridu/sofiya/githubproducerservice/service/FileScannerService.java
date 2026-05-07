package com.kafka.gridu.sofiya.githubproducerservice.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.gridu.sofiya.githubproducerservice.dto.GithubAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileScannerService implements CommandLineRunner {

    private final KafkaTemplate<String, GithubAccount> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String ACCOUNTS_TOPIC = "github_accounts";

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting file scanning process..");
        InputStream inputStream = new ClassPathResource("accounts.json").getInputStream();
        List<GithubAccount> accounts = objectMapper.readValue(inputStream, new TypeReference<>() {});

        log.info("Found {} accounts in file", accounts.size());

        for (GithubAccount account : accounts) {
            kafkaTemplate.send(ACCOUNTS_TOPIC, account.username(), account)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Sent account {} to Kafka successfully", account.username());
                        } else {
                            log.error("Failed to send account {}", account.username(), ex);
                        }
                    });
        }
    }
}
