package com.example.questdbreproducer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;


@Slf4j
@SpringBootTest
class QuestdbReproducerApplicationTests {

    @Autowired
    private QuestDB questDB;

    @Test
    void exceptionsDuringConcurrentWritesAndReads() {
        ScheduledExecutorService executorService = newScheduledThreadPool(5);
        for (int i = 0; i < 3; i++) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    Collection<Metric> dataPoints = generateMetrics();
                    questDB.insertWithTableCreation("metrics", dataPoints);
                } catch (Exception e) {
                    log.warn("WRITES: Smth went wrong", e);
                }
            }, 0, 500, MILLISECONDS);
        }
        for (int i = 0; i < 2; i++) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    questDB.queryAll();
                } catch (Exception e) {
                    log.warn("READS: Smth went wrong", e);
                }
            }, 2000, 300, MILLISECONDS);
        }
        while (true) {
        }
    }

    @Test
    void noExceptionsDuringConcurrentWritesAndReads() {
        questDB.createTableIfNotExists("metrics");
        ScheduledExecutorService executorService = newScheduledThreadPool(5);
        for (int i = 0; i < 3; i++) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    Collection<Metric> dataPoints = generateMetrics();
                    questDB.insertWithoutTableCreation("metrics", dataPoints);
                } catch (Exception e) {
                    log.warn("WRITES: Smth went wrong", e);
                }
            }, 0, 500, MILLISECONDS);
        }
        for (int i = 0; i < 2; i++) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    questDB.queryAll();
                } catch (Exception e) {
                    log.warn("READS: Smth went wrong", e);
                }
            }, 2000, 300, MILLISECONDS);
        }
        while (true) {
        }
    }

    private Collection<Metric> generateMetrics() {
        Supplier<Metric> metricGenerator = () -> Metric.builder()
                .time(System.currentTimeMillis())
                .name("any.metric")
                .value(42)
                .unit("MS")
                .tag(Tag.builder().name("mc_member").value("localhost:5701").build())
                .tag(Tag.builder().name("smth").value("else").build())
                .build();
        return Stream.generate(metricGenerator).limit(1500).collect(toList());
    }
}
