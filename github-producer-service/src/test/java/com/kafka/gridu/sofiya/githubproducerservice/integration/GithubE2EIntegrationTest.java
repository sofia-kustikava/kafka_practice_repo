package com.kafka.gridu.sofiya.githubproducerservice.integration;

import com.kafka.gridu.sofiya.githubproducerservice.dto.Commit;
import com.kafka.gridu.sofiya.githubproducerservice.dto.GithubAccount;
import com.kafka.gridu.sofiya.githubproducerservice.service.GithubService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"github_accounts", "github_commits"})
class GithubE2EIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired(required = false)
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockBean
    private GithubService githubService;

    @Test
    void shouldProcessFullFlowFromAccountToCommit() throws Exception {
        GHCommit mockGhCommit = mock(GHCommit.class);
        GHRepository mockRepo = mock(GHRepository.class);
        GHCommit.ShortInfo mockShortInfo = mock(GHCommit.ShortInfo.class);

        when(mockGhCommit.getSHA1()).thenReturn("sha-12345");
        when(mockGhCommit.getCommitDate()).thenReturn(new Date());
        when(mockGhCommit.getCommitShortInfo()).thenReturn(mockShortInfo);
        when(mockShortInfo.getMessage()).thenReturn("Test commit message");
        when(mockGhCommit.getOwner()).thenReturn(mockRepo);
        when(mockRepo.getName()).thenReturn("test-repo");
        when(mockRepo.getLanguage()).thenReturn("Java");

        when(githubService.fetchCommits(any(GithubAccount.class))).thenReturn(List.of(mockGhCommit));

        JsonDeserializer<Commit> commitDeserializer = new JsonDeserializer<>(Commit.class);
        commitDeserializer.addTrustedPackages("*");
        commitDeserializer.setUseTypeHeaders(false);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-commits", "true", embeddedKafkaBroker);
        DefaultKafkaConsumerFactory<String, Commit> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), commitDeserializer);

        Consumer<String, Commit> consumer = cf.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "github_commits");

        String testUser = "test-user";
        GithubAccount account = new GithubAccount(testUser, "1d");
        kafkaTemplate.send("github_accounts", testUser, account);

        ConsumerRecords<String, Commit> commitRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

        ConsumerRecord<String, Commit> firstRecord = commitRecords.iterator().next();
        assertThat(firstRecord).isNotNull();
        assertThat(firstRecord.key()).isEqualTo(testUser);

        Commit finalCommit = firstRecord.value();
        assertThat(finalCommit.username()).isEqualTo(testUser);
        assertThat(finalCommit.sha()).isEqualTo("sha-12345");
        assertThat(finalCommit.repositoryName()).isEqualTo("test-repo");
        assertThat(finalCommit.programmingLanguage()).isEqualTo("Java");
        assertThat(finalCommit.message()).isEqualTo("Test commit message");

        consumer.close();
    }
}
