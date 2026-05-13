package com.kafka.gridu.sofiya.streamsservice.config;

import com.kafka.gridu.sofiya.streamsservice.dto.AvgMsgAccumulator;
import com.kafka.gridu.sofiya.streamsservice.dto.Commit;
import com.kafka.gridu.sofiya.streamsservice.service.SchedulerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;

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
        JsonSerde<AvgMsgAccumulator> avgMessageSerde = new JsonSerde<>(AvgMsgAccumulator.class);

        KStream<String, Commit> commitStream = streamsBuilder.stream(
                "github_commits",
                Consumed.with(
                        Serdes.String(),
                        commitSerde)
        );

        commitStream
                .groupBy((key, value) -> "TOTAL_COMMIT_COUNT")
                .count(Materialized.as("total-store"))
                .toStream()
                .foreach((key, count) -> schedulerService.updateTotalCommits(count));

        commitStream
                .groupByKey()
                .count(Materialized.as("user-store"))
                .toStream()
                .foreach(schedulerService::updateUserCommitsCount);

        commitStream.groupBy((key, value) -> value.programmingLanguage() != null ? value.programmingLanguage() : "Unknown")
                .count(Materialized.as("lang-store"))
                .toStream()
                .foreach(schedulerService::updateLanguageStatsCount);

        commitStream
                .groupBy((key, value) -> "AVG_MESSAGE_LENGTH")
                .aggregate(
                        () -> new AvgMsgAccumulator(0, 0),
                        (key, value, aggregate) -> new AvgMsgAccumulator(
                                aggregate.count() + 1,
                                aggregate.sum() + value.message().length()
                        ),
                        Materialized.<String, AvgMsgAccumulator, KeyValueStore<Bytes, byte[]>>as("avg-message-length-store")
                                .withValueSerde(avgMessageSerde)
                ).toStream()
                .foreach((key, agg) ->
                        schedulerService.updateAvgMessageLength(agg.sum() / agg.count())
                );

        commitStream
                .filter((key, value) -> {
                    if (value.date() == null) return false;
                    LocalDate date = value.date().toInstant()
                            .atZone(ZoneId.systemDefault())
                            .toLocalDate();
                    DayOfWeek day = date.getDayOfWeek();
                    return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
                })
                .groupBy((key, value) -> "WEEKEND_COMMITS_COUNT")
                .count(Materialized.as("weekend-commits-store"))
                .toStream()
                .foreach((key, count) -> schedulerService.updateWeekendCommits(count));

        commitStream
                .mapValues(value -> {
                    java.time.LocalDate date = value.date().toInstant()
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDate();
                    return date.getDayOfWeek().toString();
                })
                .groupBy((key, dayName) -> dayName)
                .count(Materialized.as("days-activity-store"))
                .toStream()
                .foreach(schedulerService::updateMostActiveDate);

        commitStream
                .groupBy((key, value) -> value.repositoryName())
                .count(Materialized.as("repo-counts-store"))
                .toStream()
                .groupBy((key, value) -> "TOTAL_REPOS_METRIC")
                .count()
                .toStream()
                .foreach((key, count) -> schedulerService.updateTotalRepos(count));

        return commitStream;
    }

}
