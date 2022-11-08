package com.example.questdbreproducer.domain;

import lombok.Builder;

@Builder
public record Metric(
        String name,
        long value,
        long time
) {

}
