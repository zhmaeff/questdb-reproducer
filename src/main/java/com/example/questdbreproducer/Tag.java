package com.example.questdbreproducer;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class Tag implements Comparable<Tag> {

    @NonNull
    String name;
    @NonNull
    String value;

    @Override
    public int compareTo(Tag other) {
        return name.compareTo(other.name);
    }

}