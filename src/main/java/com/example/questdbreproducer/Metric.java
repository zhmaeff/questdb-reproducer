package com.example.questdbreproducer;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class Metric {
    String name;
    long value;
    @Singular
    List<Tag> tags;
    String unit;
    long time;
}
