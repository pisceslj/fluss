/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerConfig;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerGroupManager;
import org.apache.fluss.client.table.scanner.log.internals.OffsetCommitCallback;
import org.apache.fluss.client.table.scanner.log.internals.OffsetMetadata;
import org.apache.fluss.client.table.scanner.log.internals.utils.Time;
import org.apache.fluss.client.table.scanner.log.internals.utils.Timer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class that centralizes the shared poll loop, lightweight single-thread guard, wakeup and
 * close lifecycle for log scanners. Concrete scanners supply the result type {@code R} and an
 * enrichment step from raw {@link ScanRecords} to that type.
 *
 * <p>This class is NOT thread-safe. The lightweight guard only detects accidental concurrent use
 * and throws {@link ConcurrentModificationException}; it does not serialize callers.
 */
@Internal
abstract class AbstractLogScanner<R> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogScanner.class);

    private static final long NO_CURRENT_THREAD = -1L;

    protected final String name;
    protected final LogScannerStatus logScannerStatus;
    protected final LogFetcher logFetcher;
    protected final ScannerMetricGroup scannerMetricGroup;
    private final ConsumerGroupManager consumerGroupManager;
    private final Time time;
    private final long defaultApiTimeoutMs;
    private final String offsetResetPolicy;

    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing the scanner
    // and is used to prevent multithreaded access.
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread that has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);

    protected AbstractLogScanner(
            String name,
            LogScannerStatus logScannerStatus,
            LogFetcher logFetcher,
            ScannerMetricGroup scannerMetricGroup,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        this.name = name;
        this.logScannerStatus = logScannerStatus;
        this.logFetcher = logFetcher;
        this.scannerMetricGroup = scannerMetricGroup;
        this.scannerMetricGroup.registerRecordsLagGauge(logScannerStatus::recordsLag);
        this.time = Time.SYSTEM;
        this.defaultApiTimeoutMs = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
        this.offsetResetPolicy =
                conf.getString(ConsumerConfig.AUTO_OFFSET_RESET).toLowerCase(Locale.ROOT);
        if (!offsetResetPolicy.equals("earliest")
                && !offsetResetPolicy.equals("latest")
                && !offsetResetPolicy.equals("none")) {
            throw new InvalidConfigException(
                    "Unsupported "
                            + ConsumerConfig.AUTO_OFFSET_RESET.key()
                            + ": "
                            + offsetResetPolicy);
        }

        String groupId = conf.getString(ConsumerConfig.GROUP_ID);
        boolean autoCommitEnabled = conf.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT);
        if (groupId == null) {
            if (autoCommitEnabled && conf.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT.key())) {
                throw new InvalidConfigException(
                        ConsumerConfig.ENABLE_AUTO_COMMIT.key()
                                + " cannot be true without "
                                + ConsumerConfig.GROUP_ID.key());
            }
            this.consumerGroupManager = null;
        } else {
            this.consumerGroupManager =
                    new ConsumerGroupManager(
                            groupId,
                            metadataUpdater,
                            autoCommitEnabled,
                            conf.get(ConsumerConfig.AUTO_COMMIT_INTERVAL).toMillis());
        }
    }

    /**
     * Template poll loop shared by all concrete scanners.
     *
     * <ul>
     *   <li>If {@link #pollForFetches()} returns a batch with scanner progress, {@link
     *       #toResult(ScanRecords)} is invoked to produce the concrete result type.
     *   <li>If the batch has no progress, we wait on {@link LogFetcher#awaitNotEmpty(long)} until
     *       either data arrives, the timeout elapses, or {@link #wakeup()} is called.
     *   <li>Any empty/timeout/wakeup path returns {@link #emptyResult()}.
     * </ul>
     */
    public final R poll(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException(notSubscribedMessage());
            }

            Timer timer = time.timer(timeout);
            if (!initializeCommittedOffsets(timer)) {
                return emptyResult();
            }
            maybeAutoCommit();

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            long timeoutNanos = timeout.toNanos();
            long startNanos = System.nanoTime();
            do {
                ScanRecords scanRecords = pollForFetches();
                if (!scanRecords.hasProgress()) {
                    try {
                        if (!logFetcher.awaitNotEmpty(startNanos + timeoutNanos)) {
                            // no data in buffer within the timeout
                            return emptyResult();
                        }
                    } catch (WakeupException e) {
                        // wakeup() was called, return empty
                        return emptyResult();
                    }
                } else {
                    // before returning the fetched records, send off the next round of fetches
                    // to enable pipelining while the user is handling the current batch.
                    logFetcher.sendFetches();
                    return toResult(scanRecords);
                }
            } while (System.nanoTime() - startNanos < timeoutNanos);

            return emptyResult();
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

    private ScanRecords pollForFetches() {
        ScanRecords scanRecords = logFetcher.collectFetch();
        if (scanRecords.hasProgress()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();

        return logFetcher.collectFetch();
    }

    /** The empty sentinel for the concrete result type. */
    protected abstract R emptyResult();

    /** Convert a non-empty batch of {@link ScanRecords} into the concrete result type. */
    protected abstract R toResult(ScanRecords scanRecords);

    /** Error message for {@link IllegalStateException} when no buckets are subscribed. */
    protected abstract String notSubscribedMessage();

    public final void wakeup() {
        logFetcher.wakeup();
    }

    protected final void subscribeFromCommittedOffset(TableBucket tableBucket) {
        requireConsumerGroup();
        logScannerStatus.assignFromCommittedOffset(tableBucket);
    }

    protected final boolean initializeCommittedOffsets(Timer timer) {
        if (consumerGroupManager == null) {
            return true;
        }
        java.util.Set<TableBucket> missing = logScannerStatus.missingCommittedOffsets();
        if (missing.isEmpty()) {
            return true;
        }
        Map<TableBucket, OffsetMetadata> committed =
                consumerGroupManager.fetchCommittedOffsets(missing, timer);
        if (committed == null) {
            return false;
        }
        logScannerStatus.resolveCommittedOffsets(committed, offsetResetPolicy);
        return true;
    }

    protected final void maybeAutoCommit() {
        if (consumerGroupManager != null) {
            consumerGroupManager.maybeAutoCommitOffsetsAsync(
                    time.milliseconds(), logScannerStatus.allConsumed());
        }
    }

    public final void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Duration timeout) {
        commitSync(logScannerStatus.allConsumed(), timeout);
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            if (!consumerGroupManager.commitOffsetsSync(
                    Objects.requireNonNull(offsets), time.timer(timeout))) {
                throw new TimeoutException(
                        "Timed out after " + timeout.toMillis() + " ms committing offsets");
            }
        } finally {
            release();
        }
    }

    public final void commitAsync() {
        commitAsync((OffsetCommitCallback) null);
    }

    public final void commitAsync(OffsetCommitCallback callback) {
        commitAsync(logScannerStatus.allConsumed(), callback);
    }

    public final void commitAsync(
            Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            consumerGroupManager.commitOffsetsAsync(Objects.requireNonNull(offsets), callback);
        } finally {
            release();
        }
    }

    private void requireConsumerGroup() {
        if (consumerGroupManager == null) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer offset operations require " + ConsumerConfig.GROUP_ID.key());
        }
    }

    /**
     * Acquire the light lock and ensure that the scanner hasn't been closed.
     *
     * @throws IllegalStateException if the scanner has been closed
     */
    protected final void acquireAndEnsureOpen() {
        acquire();
        if (closed) {
            release();
            throw new IllegalStateException("This scanner has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this scanner from multithreaded access. Instead of blocking
     * when the lock is not available, we throw an exception (multithreaded usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get()
                && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException(
                    "Scanner is not safe for multithreaded access. "
                            + "currentThread(name: "
                            + thread.getName()
                            + ", id: "
                            + threadId
                            + ")"
                            + " otherThread(id: "
                            + currentThread.get()
                            + ")");
        }
        refCount.incrementAndGet();
    }

    /** Release the light lock protecting the scanner from multithreaded access. */
    protected final void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public final void close() {
        acquire();
        try {
            if (!closed) {
                LOG.trace("Closing log scanner: {}", name);
                if (consumerGroupManager != null) {
                    consumerGroupManager.maybeAutoCommitOffsetsSync(
                            logScannerStatus.allConsumed(), time.timer(defaultApiTimeoutMs));
                    consumerGroupManager.close();
                }
                scannerMetricGroup.close();
                logFetcher.close();
            }
            LOG.debug("Log scanner: {} has been closed", name);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log scanner: " + name, e);
        } finally {
            closed = true;
            release();
        }
    }
}
