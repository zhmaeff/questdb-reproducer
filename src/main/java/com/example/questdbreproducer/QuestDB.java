package com.example.questdbreproducer;

import io.questdb.BuildInformation;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfigurationException;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.json.JsonException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

@Slf4j
@Getter
@Service
public class QuestDB extends ServerMain {

    private static final Properties SERVERCONF = new Properties();

    static {
        SERVERCONF.setProperty("http.enabled", "true");
        SERVERCONF.setProperty("http.min.enabled", "false");
        SERVERCONF.setProperty("line.udp.enabled", "false");
        SERVERCONF.setProperty("line.tcp.enabled", "false");
        SERVERCONF.setProperty("metrics.enabled", "true");
        SERVERCONF.setProperty("telemetry.enabled", "true");
        SERVERCONF.setProperty("pg.enabled", "false");
    }

    private CairoEngine cairoEngine;
    private String rootDirectory;
    private final ConcurrentMap<String, ReentrantLock> tableLocks = new ConcurrentHashMap<>();

    public QuestDB() throws Exception {
        super(new String[]{"-d", "./questdb"});
    }


    @Override
    protected void initQuestDb(WorkerPool workerPool, CairoEngine cairoEngine, Log log) {
        this.cairoEngine = cairoEngine;
    }

    @Override
    protected void readServerConfiguration(String rootDirectory, Properties properties,
                                           Log log, BuildInformation buildInformation)
            throws ServerConfigurationException, JsonException {
        this.rootDirectory = rootDirectory;
        configuration = new CustomPropServerConfiguration(rootDirectory, SERVERCONF, emptyMap(), log, buildInformation);
    }

    public void createTableIfNotExists(String table) {
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(cairoEngine, 1);
        try (SqlCompiler compiler = new SqlCompiler(cairoEngine)) {
            compiler.compile("CREATE TABLE IF NOT EXISTS \"" + table + "\""
                    + "( time TIMESTAMP, metric SYMBOL CAPACITY 1024 index, value LONG, "
                    + "unit SYMBOL, mc_member SYMBOL index) TIMESTAMP(time) PARTITION BY DAY", ctx);
        } catch (SqlException e) {
            throw new IllegalStateException(e);
        }
    }

    public void insertWithoutTableCreation(String table, Collection<Metric> dataPoints) {
        ReentrantLock tableLock = tableLocks.computeIfAbsent(table, k -> new ReentrantLock());
        tableLock.lock();
        try {
            log.info("Writing...");
            try (TableWriter writer = cairoEngine.getWriter(AllowAllCairoSecurityContext.INSTANCE, table, "writing data points")) {
                dataPoints.forEach(point -> {
                    for (Tag tag : point.getTags()) {
                        String tagName = tag.getName();
                        if (writer.getMetadata().getColumnIndexQuiet(tagName) < 0) {
                            writer.addColumn(tagName, ColumnType.SYMBOL);
                        }
                    }
                    TableWriter.Row row = writer.newRow(point.getTime() * 1000);
                    row.putSym(1, point.getName());
                    row.putLong(2, point.getValue());
                    row.putSym(3, point.getUnit());
                    for (Tag tag : point.getTags()) {
                        row.putSym(writer.getColumnIndex(tag.getName()), tag.getValue());
                    }
                    row.append();
                });
                writer.commit();
            }
        } finally {
            tableLock.unlock();
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertWithTableCreation(String table, Collection<Metric> dataPoints) {
        ReentrantLock tableLock = tableLocks.computeIfAbsent(table, k -> new ReentrantLock());
        tableLock.lock();
        try {
            log.info("Writing...");
            createTableIfNotExists(table);
            try (TableWriter writer = cairoEngine.getWriter(AllowAllCairoSecurityContext.INSTANCE, table, "writing data points")) {
                dataPoints.forEach(point -> {
                    for (Tag tag : point.getTags()) {
                        String tagName = tag.getName();
                        if (writer.getMetadata().getColumnIndexQuiet(tagName) < 0) {
                            writer.addColumn(tagName, ColumnType.SYMBOL);
                        }
                    }
                    TableWriter.Row row = writer.newRow(point.getTime() * 1000);
                    row.putSym(1, point.getName());
                    row.putLong(2, point.getValue());
                    row.putSym(3, point.getUnit());
                    for (Tag tag : point.getTags()) {
                        row.putSym(writer.getColumnIndex(tag.getName()), tag.getValue());
                    }
                    row.append();
                });
                writer.commit();
            }
        } finally {
            tableLock.unlock();
        }
    }

    public Optional<DataPoint> queryLatest() {
        String queryString = "SELECT time, value from metrics ORDER BY time DESC LIMIT 1";
        return doQuery(queryString, cursor -> {
            if (cursor.hasNext()) {
                final Record rowRecord = cursor.getRecord();
                return Optional.of(DataPoint.builder()
                        .time(rowRecord.getTimestamp(0))
                        .value(rowRecord.getLong(1))
                        .build());
            } else {
                return Optional.empty();
            }
        });
    }

    public List<DataPoint> queryAll() {
        String queryString = "SELECT time, value from metrics ORDER BY time ASC";
        return doQuery(queryString, cursor -> {
            if (!cursor.hasNext()) {
                return emptyList();
            }
            List<DataPoint> result = new ArrayList<>();
            while (cursor.hasNext()) {
                Record rowRecord = cursor.getRecord();
                result.add(DataPoint.builder()
                        .time(rowRecord.getTimestamp(0))
                        .value(rowRecord.getLong(1))
                        .build());
            }
            return result;
        });
    }

    private <T> T doQuery(String query, Function<RecordCursor, T> mapper) {
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(cairoEngine, 1);
        try (SqlCompiler compiler = new SqlCompiler(cairoEngine)) {
            log.info("Executing SQL '{}'", query);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    return mapper.apply(cursor);
                }
            }
        } catch (SqlException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class CustomPropServerConfiguration extends PropServerConfiguration {

        public CustomPropServerConfiguration(String root, Properties properties,
                                             Map<String, String> env, Log log,
                                             BuildInformation buildInformation)
                throws ServerConfigurationException, JsonException {
            super(root, properties, env, log, buildInformation);
        }

        @Override
        @SuppressWarnings("checkstyle:AnonInnerLength")
        public WorkerPoolConfiguration getWorkerPoolConfiguration() {
            WorkerPoolConfiguration workerPoolConfiguration = super.getWorkerPoolConfiguration();
            return new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return workerPoolConfiguration.getWorkerAffinity();
                }

                @Override
                public int getWorkerCount() {
                    return workerPoolConfiguration.getWorkerCount();
                }

                @Override
                public boolean haltOnError() {
                    return workerPoolConfiguration.haltOnError();
                }

                @Override
                public long getYieldThreshold() {
                    return workerPoolConfiguration.getYieldThreshold();
                }

                @Override
                public long getSleepThreshold() {
                    return workerPoolConfiguration.getSleepThreshold();
                }

                // WHOLE CONFIG CLASS FOR THIS PROPERTY
                @Override
                public boolean isDaemonPool() {
                    return true;
                }
            };
        }
    }
}
