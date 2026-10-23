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

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for consumer group Admin API result models. */
class ConsumerGroupAdminResultTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, null, 0);

    @Test
    void testConsumerGroupOffsetsResultIsImmutable() {
        Map<TableBucket, ConsumerGroupOffsetsResult.Entry> entries = new LinkedHashMap<>();
        entries.put(TABLE_BUCKET, new ConsumerGroupOffsetsResult.Entry(42L, null));
        ConsumerGroupOffsetsResult result = new ConsumerGroupOffsetsResult("group-1", entries);
        entries.clear();

        assertThat(result.groupId()).isEqualTo("group-1");
        assertThat(result.entries().get(TABLE_BUCKET).committedOffset()).isEqualTo(42L);
        assertThat(result.entries().get(TABLE_BUCKET).metadata()).isEmpty();
        assertThatThrownBy(() -> result.entries().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testAlterConsumerOffsetsResultIsImmutable() {
        AlterConsumerOffsetsResult.BucketOutcome outcome =
                new AlterConsumerOffsetsResult.BucketOutcome(12L, null, null);
        AlterConsumerOffsetsResult result =
                new AlterConsumerOffsetsResult(Collections.singletonMap(TABLE_BUCKET, outcome));

        assertThat(result.outcomes().get(TABLE_BUCKET).isSuccess()).isTrue();
        assertThat(result.outcomes().get(TABLE_BUCKET).newOffset()).isEqualTo(12L);
        assertThatThrownBy(() -> result.outcomes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDeleteConsumerGroupsAggregatesFutures() {
        CompletableFuture<Void> first = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> second = new CompletableFuture<>();
        Map<String, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put("group-1", first);
        futures.put("group-2", second);
        DeleteConsumerGroupsResult result = new DeleteConsumerGroupsResult(futures);

        assertThat(result.deletedGroup("group-1")).isSameAs(first);
        assertThat(result.all()).isNotDone();
        second.complete(null);
        assertThat(result.all()).isCompleted();
        assertThatThrownBy(() -> result.perGroupFutures().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDeleteConsumerOffsetsAggregatesFutures() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        DeleteConsumerGroupOffsetsResult result =
                new DeleteConsumerGroupOffsetsResult(
                        Collections.singletonMap(TABLE_BUCKET, future));

        assertThat(result.bucketResult(TABLE_BUCKET)).isSameAs(future);
        assertThat(result.all()).isNotDone();
        future.complete(null);
        assertThat(result.all()).isCompleted();
    }

    @Test
    void testListOffsetsOptionsCopiesBuckets() {
        java.util.Set<TableBucket> buckets = new java.util.LinkedHashSet<>();
        buckets.add(TABLE_BUCKET);
        ListConsumerGroupOffsetsOptions options =
                new ListConsumerGroupOffsetsOptions().buckets(buckets);
        buckets.clear();

        assertThat(options.buckets()).containsExactly(TABLE_BUCKET);
        assertThatThrownBy(() -> options.buckets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
