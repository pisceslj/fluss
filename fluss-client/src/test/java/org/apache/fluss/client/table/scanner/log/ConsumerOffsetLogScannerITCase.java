/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.ConsumerGroupListing;
import org.apache.fluss.client.admin.ConsumerGroupOffsetsResult;
import org.apache.fluss.client.admin.ListConsumerGroupOffsetsOptions;
import org.apache.fluss.client.admin.ListConsumerGroupsOptions;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerConfig;
import org.apache.fluss.client.table.scanner.log.internals.OffsetMetadata;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for consumer offsets exposed through {@link LogScanner}. */
class ConsumerOffsetLogScannerITCase extends ClientToServerITCaseBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .build();

    @BeforeEach
    void waitForConsumerOffsetsTable() throws Exception {
        // Keep scanner tests independent of system-table creation and assignment timing.
        long tableId =
                createTable(
                        TablePath.of("sys", "consumer_offsets"),
                        TableDescriptor.builder()
                                .schema(ConsumerOffsetSchema.getTableSchema())
                                .logFormat(LogFormat.COMPACTED)
                                .distributedBy(1, ConsumerOffsetSchema.getBucketKeyColumn())
                                .build(),
                        true);
        waitAllReplicasReady(tableId, 1);
    }

    @Test
    void testCommitAndResumeFromCommittedOffset() throws Exception {
        TablePath tablePath = uniqueTablePath("resume");
        long tableId = createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 5);
        String groupId = "scanner-resume-" + System.nanoTime();
        TableBucket tableBucket = new TableBucket(tableId, null, 0);

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            commitAsyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            commitSyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(3L, "checkpoint")));
        }

        ConsumerGroupOffsetsResult committed = waitForOffset(groupId, tableBucket, 3L);
        assertThat(committed.entries().get(tableBucket).committedOffset).isEqualTo(3L);
        assertThat(committed.entries().get(tableBucket).metadata).isEqualTo("checkpoint");

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            int records = 0;
            long deadline = System.currentTimeMillis() + 10_000L;
            while (records < 2 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = scanner.poll(Duration.ofMillis(200));
                for (ScanRecord ignored : scanRecords) {
                    records++;
                }
            }
            assertThat(records).isEqualTo(2);
        }
    }

    @Test
    void testAsyncCommitAndAdminLifecycle() throws Exception {
        TablePath tablePath = uniqueTablePath("admin");
        long tableId = createTable(tablePath, descriptor(2), false);
        appendRows(tablePath, 8);
        String groupId = "scanner-admin-" + System.nanoTime();
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(new TableBucket(tableId, null, 0), new OffsetMetadata(2L));
        offsets.put(new TableBucket(tableId, null, 1), new OffsetMetadata(3L));

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            scanner.subscribeFromBeginning(1);
            AtomicReference<Exception> callbackFailure = commitAsyncWithRetry(scanner, offsets);
            assertThat(callbackFailure.get()).isNull();
        }

        assertThat(admin.listConsumerGroups(new ListConsumerGroupsOptions()).get())
                .extracting(ConsumerGroupListing::groupId)
                .contains(groupId);
        assertThat(committedOffsets(groupId).entries()).hasSize(2);

        TableBucket deleted = new TableBucket(tableId, null, 0);
        admin.deleteConsumerGroupOffsets(groupId, Collections.singleton(deleted)).all().get();
        assertThat(committedOffsets(groupId).entries())
                .doesNotContainKey(deleted)
                .containsKey(new TableBucket(tableId, null, 1));

        admin.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
        waitForEmptyOffsets(groupId);
    }

    @Test
    void testClosePerformsFinalAutoCommit() throws Exception {
        TablePath tablePath = uniqueTablePath("close");
        long tableId = createTable(tablePath, descriptor(1), false);
        String groupId = "scanner-close-" + System.nanoTime();
        TableBucket tableBucket = new TableBucket(tableId, null, 0);

        try (Connection groupConnection = groupConnection(groupId, true);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribe(0, 0L);
            commitAsyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            scanner.subscribe(0, 7L);
        }

        assertThat(
                        waitForOffset(groupId, tableBucket, 7L)
                                .entries()
                                .get(tableBucket)
                                .committedOffset)
                .isEqualTo(7L);
    }

    private Connection groupConnection(String groupId, boolean autoCommit) {
        Configuration configuration = new Configuration(clientConf);
        configuration.setString(ConsumerConfig.GROUP_ID.key(), groupId);
        configuration.setBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT.key(), autoCommit);
        return ConnectionFactory.createConnection(configuration);
    }

    private ConsumerGroupOffsetsResult committedOffsets(String groupId) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsumerGroupOffsetsResult result =
                        admin.listConsumerGroupOffsets(
                                        groupId, new ListConsumerGroupOffsetsOptions())
                                .get();
                if (!result.entries().isEmpty()) {
                    return result;
                }
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Offsets did not become available for " + groupId, lastFailure);
    }

    private ConsumerGroupOffsetsResult waitForOffset(
            String groupId, TableBucket tableBucket, long expectedOffset) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsumerGroupOffsetsResult result =
                        admin.listConsumerGroupOffsets(
                                        groupId, new ListConsumerGroupOffsetsOptions())
                                .get();
                ConsumerGroupOffsetsResult.Entry entry = result.entries().get(tableBucket);
                if (entry != null && entry.committedOffset == expectedOffset) {
                    return result;
                }
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Offset " + expectedOffset + " did not become available for " + groupId,
                lastFailure);
    }

    private void waitForEmptyOffsets(String groupId) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsumerGroupOffsetsResult result =
                        admin.listConsumerGroupOffsets(
                                        groupId, new ListConsumerGroupOffsetsOptions())
                                .get();
                if (result.entries().isEmpty()) {
                    return;
                }
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Offsets were not deleted for " + groupId, lastFailure);
    }

    private static void commitSyncWithRetry(
            LogScanner scanner, Map<TableBucket, OffsetMetadata> offsets) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (true) {
            try {
                scanner.commitSync(offsets);
                return;
            } catch (RuntimeException failure) {
                if (System.currentTimeMillis() >= deadline) {
                    throw failure;
                }
                Thread.sleep(200L);
            }
        }
    }

    private static AtomicReference<Exception> commitAsyncWithRetry(
            LogScanner scanner, Map<TableBucket, OffsetMetadata> offsets) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        AtomicReference<Exception> failure = new AtomicReference<>();
        while (System.currentTimeMillis() < deadline) {
            CountDownLatch callback = new CountDownLatch(1);
            failure.set(null);
            scanner.commitAsync(
                    offsets,
                    (committed, error) -> {
                        failure.set(error);
                        callback.countDown();
                    });
            while (callback.getCount() > 0 && System.currentTimeMillis() < deadline) {
                scanner.poll(Duration.ofMillis(100));
            }
            if (callback.await(1, TimeUnit.SECONDS) && failure.get() == null) {
                return failure;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Asynchronous offset commit did not succeed", failure.get());
    }

    private void appendRows(TablePath tablePath, int count) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < count; i++) {
                writer.append(GenericRow.of(i, BinaryString.fromString("v" + i)));
            }
            writer.flush();
        }
    }

    private static TableDescriptor descriptor(int buckets) {
        return TableDescriptor.builder().schema(SCHEMA).distributedBy(buckets, "id").build();
    }

    private static TablePath uniqueTablePath(String suffix) {
        return TablePath.of("consumer_offset_it", suffix + "_" + System.nanoTime());
    }
}
