package com.kafka.gridu.sofiya.streamsservice.dto;

public record AvgMsgAccumulator(
        long count,
        long sum) {
}
