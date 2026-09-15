package vdf.vdt.streaming.generator.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Monitors and logs data generation throughput (TPS) periodically.
 * Prints actual vs target TPS, success/failure counts, and alerts if throughput falls below target.
 */
public class ThroughputTracker {

    private final LongAdder successCount = new LongAdder();
    private final LongAdder failCount = new LongAdder();

    private long totalSuccess = 0;
    private long totalFail = 0;
    private long startTimeMs = 0;

    private ScheduledExecutorService scheduler;
    private int targetReqPerSec;
    private int logIntervalSeconds;

    public ThroughputTracker() {
        this(5); // Default 5 seconds log interval
    }

    public ThroughputTracker(int logIntervalSeconds) {
        this.logIntervalSeconds = logIntervalSeconds;
    }

    public void recordSuccess() {
        successCount.increment();
    }

    public void recordFailure() {
        failCount.increment();
    }

    public synchronized void start(int targetReqPerSec) {
        this.targetReqPerSec = targetReqPerSec;
        this.startTimeMs = System.currentTimeMillis();
        this.successCount.reset();
        this.failCount.reset();
        this.totalSuccess = 0;
        this.totalFail = 0;

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throughput-tracker");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::printMetrics, logIntervalSeconds, logIntervalSeconds, TimeUnit.SECONDS);
        System.out.printf(">>> ThroughputTracker started | Target TPS: %,d | Log Interval: %ds%n",
                targetReqPerSec, logIntervalSeconds);
    }

    private void printMetrics() {
        long intervalSuccess = successCount.sumThenReset();
        long intervalFail = failCount.sumThenReset();

        totalSuccess += intervalSuccess;
        totalFail += intervalFail;

        double actualTps = (double) intervalSuccess / logIntervalSeconds;
        long totalElapsedSec = Math.max(1, (System.currentTimeMillis() - startTimeMs) / 1000);
        double avgTps = (double) totalSuccess / totalElapsedSec;

        double pctOfTarget = targetReqPerSec > 0 ? (actualTps * 100.0 / targetReqPerSec) : 100.0;

        System.out.printf("[METRICS] Actual TPS: %,.0f req/s | Target: %,d req/s (%.1f%%) | Avg TPS: %,.0f | Total Sent: %,d | Failures: %,d%n",
                actualTps, targetReqPerSec, pctOfTarget, avgTps, totalSuccess, totalFail);

        // Print warning if actual throughput falls below 90% of target
        if (targetReqPerSec > 0 && actualTps < targetReqPerSec * 0.90) {
            System.err.printf("[WARNING] Throughput bottleneck detected! Current TPS (%,.0f) is below 90%% of target (%,d).%n",
                    actualTps, targetReqPerSec);
        }
    }

    public synchronized void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        printMetrics();
    }
}
