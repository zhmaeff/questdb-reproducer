package com.example.questdbreproducer.storage;

import com.example.questdbreproducer.domain.Metric;
import com.example.questdbreproducer.exception.ClosedStorageException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
public class QuestDBMetricsStorage implements InitializingBean, AutoCloseable {

    public static final String CLUSTER_NAME = "Cluster-1";

    private final QuestDBContext questDBContext;
    private final BlockingQueue<Collection<Metric>> queue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService persistJob = newSingleThreadScheduledExecutor(
            r -> new Thread(r, "QuestDB persist Job")
    );
    private volatile boolean closed;

    public QuestDBMetricsStorage() throws SqlException, IOException {
        this.questDBContext = new QuestDBContext(Paths.get("."));
        doQuery("""
                CREATE TABLE IF NOT EXISTS "%s" (
                 time TIMESTAMP,
                 metric SYMBOL INDEX,
                 value LONG
                ) TIMESTAMP(time) PARTITION BY HOUR""".formatted(CLUSTER_NAME));
    }

    @Override
    public void afterPropertiesSet() {
        persistJob.scheduleAtFixedRate(() -> {
            List<Collection<Metric>> metricsCollections = new ArrayList<>();
            queue.drainTo(metricsCollections);
            metricsCollections.forEach(this::persist);
        }, 10, 10, MILLISECONDS);
    }

    private void persist(Collection<Metric> dataPoints) {
        try (
                SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(questDBContext.getCairoEngine(), 1);
                TableWriter writer = questDBContext.getCairoEngine().getWriter(
                        ctx.getCairoSecurityContext(),
                        CLUSTER_NAME,
                        "writing data points"
                )
        ) {
            for (Metric metric : dataPoints) {
                TableWriter.Row row = writer.newRow(metric.time());
                row.putSym(1, metric.name());
                row.putLong(2, metric.value());
                row.append();
            }
            writer.commit();
        }
    }

    public void store(Collection<Metric> dataPoints) {
        if (dataPoints == null) {
            throw new IllegalArgumentException("Data points collection must be set");
        }
        if (closed) {
            throw new ClosedStorageException();
        }
        queue.add(dataPoints);
    }

    public Optional<Metric> selectLatest(@NonNull String metricName) {
        if (closed) {
            throw new ClosedStorageException();
        }
        return doQuery("""
                SELECT time, metric, value FROM "%s" WHERE metric = '%s' ORDER by time DESC LIMIT 1
                """.formatted(CLUSTER_NAME, metricName), cursor -> {
            Optional<Metric> result;
            if (cursor.hasNext()) {
                final Record rowRecord = cursor.getRecord();
                result = Optional.of(Metric.builder()
                        .time(rowRecord.getTimestamp(0))
                        .name(rowRecord.getSym(1).toString())
                        .value(rowRecord.getLong(2))
                        .build());
            } else {
                result = Optional.empty();
            }
            return result;
        });
    }

    private void doQuery(String query) {
        doQuery(query, it -> null);
    }

    private <T> T doQuery(String query, Function<RecordCursor, T> mapper) {
        log.debug("Executing SQL: {}", query);
        try (
                SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(questDBContext.getCairoEngine(), 1);
                SqlCompiler compiler = new SqlCompiler(questDBContext.getCairoEngine());
                RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()
        ) {
            if (factory == null) {
                return null;
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return mapper.apply(cursor);
            }
        } catch (SqlException e) {
            log.warn("Couldn't execute SQL", e);
            throw new IllegalStateException(e);
        }
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            closed = true;
            if (!persistJob.awaitTermination(1, SECONDS)) {
                persistJob.shutdown();
            }
            questDBContext.close();
            log.debug("Closed metrics storage successfully.");
        } catch (Exception e) {
            log.error("Could not close metrics storage.", e);
        }
    }

}
