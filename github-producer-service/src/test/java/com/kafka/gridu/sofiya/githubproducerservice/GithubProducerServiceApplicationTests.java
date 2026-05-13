package com.kafka.gridu.sofiya.githubproducerservice;

import com.kafka.gridu.sofiya.githubproducerservice.service.GithubService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
class GithubProducerServiceApplicationTests {

    @MockBean
    private GithubService githubService;

    @Test
    void contextLoads() {
    }

}
