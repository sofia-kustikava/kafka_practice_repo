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
import java.time.ZoneId;
import java.util.Date;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    private final SchedulerService schedulerService;

    @Bean
    public KStream<String, Commit> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, Commit> commitStream = streamsBuilder.stream(
                "github_commits",
                Consumed.with(Serdes.String(), commitSerde())
        );
        processBasicMetrics(commitStream);
        processAverageMessageLength(commitStream);
        processWeekendActivity(commitStream);
        processDailyActivity(commitStream);
        processRepositoryMetrics(commitStream);

        return commitStream;
    }

    private void processBasicMetrics(KStream<String, Commit> stream) {
        stream.groupBy((k, v) -> "TOTAL_COUNT")
                .count(Materialized.as("total-store"))
                .toStream().foreach((k, c) -> schedulerService.updateTotalCommits(c));

        stream.groupByKey()
                .count(Materialized.as("user-store"))
                .toStream().foreach(schedulerService::updateTotalContributorsCount);

        stream.groupBy((k, v) -> v.programmingLanguage() != null ? v.programmingLanguage() : "Unknown")
                .count(Materialized.as("lang-store"))
                .toStream().foreach(schedulerService::updateLanguageStatsCount);
    }

    private void processAverageMessageLength(KStream<String, Commit> stream) {
        stream.groupBy((k, v) -> "AVG_LEN")
                .aggregate(
                        () -> new AvgMsgAccumulator(0, 0),
                        (k, v, agg) -> {
                            long len = (v.message() != null) ? v.message().length() : 0;
                            return new AvgMsgAccumulator(agg.count() + 1, agg.sum() + len);
                        },
                        Materialized.<String, AvgMsgAccumulator, KeyValueStore<Bytes, byte[]>>as("avg-len-store")
                                .withValueSerde(new JsonSerde<>(AvgMsgAccumulator.class))
                )
                .toStream()
                .foreach((k, agg) -> schedulerService.updateAvgMessageLength(agg.sum() / agg.count()));
    }

    private void processWeekendActivity(KStream<String, Commit> stream) {
        stream.filter((k, v) -> isWeekend(v.date()))
                .groupBy((k, v) -> "WEEKEND_COUNT")
                .count(Materialized.as("weekend-store"))
                .toStream().foreach((k, c) -> schedulerService.updateWeekendCommits(c));
    }

    private void processDailyActivity(KStream<String, Commit> stream) {
        stream.mapValues(v -> getDayOfWeekName(v.date()))
                .groupBy((k, day) -> day)
                .count(Materialized.as("days-activity-store"))
                .toStream().foreach(schedulerService::updateMostActiveDate);
    }

    private void processRepositoryMetrics(KStream<String, Commit> stream) {
        stream.groupBy((k, v) -> v.repositoryName() != null ? v.repositoryName() : "Unknown")
                .count(Materialized.as("repo-counts-store"))
                .toStream()
                .groupBy((k, v) -> "TOTAL_REPOS")
                .count()
                .toStream().foreach((k, c) -> schedulerService.updateTotalRepos(c));
    }

    private boolean isWeekend(Date date) {
        if (date == null) return false;
        DayOfWeek day = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
    }

    private String getDayOfWeekName(Date date) {
        if (date == null) return "UNKNOWN";
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getDayOfWeek().toString();
    }

    private JsonSerde<Commit> commitSerde() {
        JsonDeserializer<Commit> des = new JsonDeserializer<>(Commit.class);
        des.addTrustedPackages("*");
        des.setUseTypeHeaders(false);
        return new JsonSerde<>(new JsonSerializer<>(), des);
    }
}