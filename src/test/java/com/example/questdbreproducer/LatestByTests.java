package com.example.questdbreproducer;

import io.questdb.cairo.sql.Record;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
class LatestByTests {

    @Autowired
    private QuestDB questDB;

    @Test
    @DisplayName("io.questdb.cairo.CairoException: [0] Not indexed: value")
    void latestByThrowsCairoException() {
        List<Metric> metrics = new ArrayList<>();
        long time = System.currentTimeMillis();

        Tag member1 = Tag.builder().name("mc_member").value("1").build();
        Tag member2 = Tag.builder().name("mc_member").value("2").build();
        Tag member3 = Tag.builder().name("mc_member").value("3").build();

        metrics.add(Metric.builder().time(time).name("memory.committedHeap").value(200).tag(member1).build());
        metrics.add(Metric.builder().time(time).name("memory.committedHeap").value(400).tag(member2).build());
        metrics.add(Metric.builder().time(time).name("memory.committedHeap").value(600).tag(member3).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.committedHeap").value(100).tag(member1).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.committedHeap").value(200).tag(member2).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.committedHeap").value(300).tag(member3).build());

        metrics.add(Metric.builder().time(time).name("memory.usedHeap").value(100).tag(member1).build());
        metrics.add(Metric.builder().time(time).name("memory.usedHeap").value(200).tag(member2).build());
        metrics.add(Metric.builder().time(time).name("memory.usedHeap").value(300).tag(member3).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.usedHeap").value(100).tag(member1).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.usedHeap").value(200).tag(member2).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.usedHeap").value(300).tag(member3).build());

        metrics.add(Metric.builder().time(time).name("memory.maxHeap").value(1000).tag(member1).build());
        metrics.add(Metric.builder().time(time).name("memory.maxHeap").value(2000).tag(member2).build());
        metrics.add(Metric.builder().time(time).name("memory.maxHeap").value(3000).tag(member3).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.maxHeap").value(1000).tag(member1).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.maxHeap").value(2000).tag(member2).build());
        metrics.add(Metric.builder().time(time - 1000).name("memory.maxHeap").value(3000).tag(member3).build());
        questDB.insertWithTableCreation("test", metrics);

        String actual = questDB.doQuery("SELECT metric, sum(value) FROM test LATEST BY mc_member WHERE mc_member IN('1', '3') AND metric IN('memory.usedHeap', 'memory.committedHeap')", cursor -> {
            StringBuilder builder = new StringBuilder();
            while (cursor.hasNext()) {
                Record record = cursor.getRecord();
                builder.append(record.getSym(0)).append(" = ").append(record.getLong(1)).append("|");
            }
            return builder.toString();
        });

        assertThat(actual).isEqualTo("memory.usedHeap = 400|memory.committedHeap = 800");

    }
}
