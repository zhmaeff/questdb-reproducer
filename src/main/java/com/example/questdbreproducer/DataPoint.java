package com.example.questdbreproducer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DataPoint {
    long time;
    long value;
}
