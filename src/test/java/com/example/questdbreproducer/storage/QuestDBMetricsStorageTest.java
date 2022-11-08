package com.example.questdbreproducer.storage;

import com.example.questdbreproducer.domain.Metric;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@SpringBootTest
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
class QuestDBMetricsStorageTest {

    @Autowired
    private QuestDBMetricsStorage metricsStorage;

    @Test
    void test() {
        for (int i = 0; i < 10; i++) {
            Metric metric = Metric.builder()
                    .time(MILLISECONDS.toMicros(Instant.now().toEpochMilli()))
                    .name("cpuUsage")
                    .value(1L)
                    .build();
            metricsStorage.store(List.of(metric));
            await().atMost(1, SECONDS).pollDelay(50, MILLISECONDS).untilAsserted(() -> {
                assertThat(metricsStorage.selectLatest("cpuUsage")).hasValue(metric);
            });
        }
    }

    @Test
    void test2() {
        test();
    }

    @Test
    void test3() {
        test();
    }

    @Test
    void test4() {
        test();
    }

    @Test
    void test5() {
        test();
    }

}