package com.example.questdbreproducer.storage;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.O3Utils;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;

import static io.questdb.log.LogFactory.DEFAULT_CONFIG_NAME;

@Slf4j
public class QuestDBContext implements AutoCloseable {

    @Getter
    private final CairoEngine cairoEngine;
    private final WorkerPool workerPool;

    public QuestDBContext(Path mcHome) throws SqlException, IOException {
        Path dbRoot = mcHome.resolve("questdb");
        Path confDir = dbRoot.resolve("conf");
        Path dataDir = dbRoot.resolve("data");
        Files.createDirectories(confDir);
        Files.createDirectories(dataDir);
        Path logConfig = confDir.resolve(DEFAULT_CONFIG_NAME);
        if (logConfig.toFile().createNewFile()) {
            Files.writeString(logConfig, """
                    writers=file,stdout
                    
                    # rolling file writer
                    w.file.class=io.questdb.log.LogRollingFileWriter
                    w.file.location=${log.dir}/questdb-rolling.log.${date:yyyyMMdd}
                    w.file.level=INFO,ERROR
                    
                    # rollEvery accepts: day, hour, minute, month
                    w.file.rollEvery=day
                    w.file.rollSize=1g
                    
                    # stdout
                    w.stdout.class=io.questdb.log.LogConsoleWriter
                    w.stdout.level=ERROR
                    """);
        }
        LogFactory.configureFromSystemProperties(new LogFactory(), dbRoot.toAbsolutePath().toString());
        DefaultCairoConfiguration cairoConfiguration = new DefaultCairoConfiguration(dataDir.toString());
        cairoEngine = new CairoEngine(cairoConfiguration);
        WorkerPool pool = new WorkerPool(() -> Math.max(Runtime.getRuntime().availableProcessors() - 1, 2));
        pool.assign(cairoEngine.getEngineMaintenanceJob());
        pool.assign(new ColumnIndexerJob(cairoEngine.getMessageBus()));
        pool.assign(new GroupByJob(cairoEngine.getMessageBus()));
        pool.assign(new LatestByAllIndexedJob(cairoEngine.getMessageBus()));
        FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache(
                cairoEngine.getConfiguration(),
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        O3Utils.setupWorkerPool(
                pool,
                cairoEngine,
                new DefaultSqlExecutionCircuitBreakerConfiguration(),
                functionFactoryCache
        );
        workerPool = pool;
        workerPool.freeOnExit(cairoEngine);
        workerPool.start();
        log.debug("QuestDB has been started");
    }

    @Override
    public void close() {
        workerPool.close();
        LogFactory.closeInstance();
        log.debug("QuestDB has been closed");
    }

}
