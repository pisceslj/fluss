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

package org.apache.fluss.client.admin;

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerConfig;
import org.apache.fluss.client.table.scanner.log.internals.OffsetMetadata;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that committed consumer offsets survive an offsets-table leader failover. */
public class ConsumerGroupFailoverITCase extends ClientToServerITCaseBase {

    @Test
    void testCommittedOffsetSurvivesOwnerFailover() throws Exception {
        TablePath tablePath =
                TablePath.of("consumer_offset_failover", "table_" + System.nanoTime());
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("value", DataTypes.STRING())
                                                .build())
                                .distributedBy(1, "id")
                                .build(),
                        false);
        String groupId = "failover-group-" + System.nanoTime();
        TableBucket dataBucket = new TableBucket(tableId, null, 0);

        Configuration groupConfiguration = new Configuration(clientConf);
        groupConfiguration.setString(ConsumerConfig.GROUP_ID.key(), groupId);
        groupConfiguration.setBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT.key(), false);
        try (Connection groupConnection = ConnectionFactory.createConnection(groupConfiguration);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            scanner.commitSync(
                    Collections.singletonMap(dataBucket, new OffsetMetadata(17L, "before")));
        }

        ConsumerGroupOffsetsResult before = waitForOffsets(admin, groupId);
        assertThat(before.entries().get(dataBucket).committedOffset).isEqualTo(17L);

        long offsetsTableId =
                admin.getTableInfo(TablePath.of("sys", "consumer_offsets")).get().getTableId();
        int bucketCount =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTableAssignment(offsetsTableId)
                        .orElseThrow(() -> new AssertionError("consumer offsets table is missing"))
                        .getBucketAssignments()
                        .size();
        waitAllReplicasReady(offsetsTableId, bucketCount);

        int bucketId =
                FlussBucketingFunction.bucketForRowKey(
                        new ConsumerOffsetSchema().encodeGroupBucketKey(groupId), bucketCount);
        TableBucket offsetsBucket = new TableBucket(offsetsTableId, null, bucketId);
        int oldLeader =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getLeaderAndIsr(offsetsBucket)
                        .orElseThrow(() -> new AssertionError("offsets bucket has no leader"))
                        .leader();

        FLUSS_CLUSTER_EXTENSION.restartTabletServer(oldLeader, new Configuration());
        int newLeader = waitUntilLeaderChanges(offsetsBucket, oldLeader);
        assertThat(newLeader).isNotEqualTo(oldLeader);

        try (Connection freshConnection = ConnectionFactory.createConnection(clientConf);
                Admin freshAdmin = freshConnection.getAdmin()) {
            ConsumerGroupOffsetsResult after = waitForOffsets(freshAdmin, groupId);
            assertThat(after.entries().get(dataBucket).committedOffset).isEqualTo(17L);
            assertThat(after.entries().get(dataBucket).metadata).isEqualTo("before");
            freshAdmin.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
        }
    }

    private int waitUntilLeaderChanges(TableBucket bucket, int oldLeader) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<LeaderAndIsr> leaderAndIsr =
                    FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getLeaderAndIsr(bucket);
            if (leaderAndIsr.isPresent() && leaderAndIsr.get().leader() != oldLeader) {
                return leaderAndIsr.get().leader();
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Leader did not change for " + bucket);
    }

    private ConsumerGroupOffsetsResult waitForOffsets(Admin targetAdmin, String groupId)
            throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsumerGroupOffsetsResult result =
                        targetAdmin
                                .listConsumerGroupOffsets(
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
}
