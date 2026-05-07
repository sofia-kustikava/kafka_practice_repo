package com.kafka.gridu.sofiya.githubproducerservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic accountsTopic() {
        return TopicBuilder.name("github_accounts")
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Bean
    public NewTopic commitsTopic() {
        return TopicBuilder.name("github_commits")
                .partitions(3)
                .replicas(3)
                .build();
    }
}
