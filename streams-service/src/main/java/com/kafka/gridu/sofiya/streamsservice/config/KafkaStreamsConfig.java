package com.kafka.gridu.sofiya.streamsservice.config;

import com.kafka.gridu.sofiya.streamsservice.dto.Commit;
import com.kafka.gridu.sofiya.streamsservice.service.SchedulerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    private final SchedulerService schedulerService;

    @Bean
    public KStream<String, Commit> kStream(StreamsBuilder streamsBuilder) {
        JsonDeserializer<Commit> deserializer = new JsonDeserializer<>(Commit.class);
        deserializer.setUseTypeHeaders(false);
        deserializer.addTrustedPackages("*");

        JsonSerde<Commit> commitSerde = new JsonSerde<>(new JsonSerializer<>(), deserializer);
        KStream<String, Commit> commitStream = streamsBuilder.stream(
                "github_commits",
                Consumed.with(
                        Serdes.String(),
                        commitSerde)
        );

        commitStream
                .groupBy((key, value) -> "TOTAL_COUNT")
                .count(Materialized.as("total-store"))
                .toStream()
                .foreach((key, count) -> schedulerService.updateTotal(count));

        commitStream
                .groupByKey()
                .count(Materialized.as("user-store"))
                .toStream()
                .foreach(schedulerService::updateUserCount);

        return commitStream;
    }

}
