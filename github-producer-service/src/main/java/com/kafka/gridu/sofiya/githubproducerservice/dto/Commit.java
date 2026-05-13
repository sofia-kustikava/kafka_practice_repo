package com.kafka.gridu.sofiya.githubproducerservice.dto;

import java.util.Date;

public record Commit(
        String username,
        String sha,
        String message,
        Date date,
        String programmingLanguage,
        String repositoryName
) {
}
