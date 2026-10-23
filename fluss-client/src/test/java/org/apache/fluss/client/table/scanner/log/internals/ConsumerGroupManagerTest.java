/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.fluss.client.table.scanner.log.internals;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.log.internals.utils.Time;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConsumerGroupManager}. */
class ConsumerGroupManagerTest {

    @Test
    void testOwnerDiscoveryIsCachedAndCanBeInvalidated() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<ServerNode> owner = new AtomicReference<>(TestingMetadataUpdater.NODE2);
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        requests.incrementAndGet();
                        ServerNode currentOwner = owner.get();
                        return CompletableFuture.completedFuture(ownerResponse(currentOwner));
                    }
                };
        TestingMetadataUpdater metadataUpdater = metadataUpdater(gateway);
        ConsumerGroupManager manager = new ConsumerGroupManager("group-1", metadataUpdater);

        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE2);
        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE2);
        assertThat(requests).hasValue(1);
        assertThat(manager.cachedOwnerGateway())
                .isSameAs(
                        metadataUpdater.newTabletServerClientForNode(
                                TestingMetadataUpdater.NODE2.id()));

        owner.set(TestingMetadataUpdater.NODE3);
        manager.markOwnerUnknown();
        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE3);
        assertThat(requests).hasValue(2);
    }

    @Test
    void testConcurrentDiscoverySharesOneRequest() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        requests.incrementAndGet();
                        return rpcResponse;
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        int callers = 4;
        CyclicBarrier startBarrier = new CyclicBarrier(callers);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<CompletableFuture<ServerNode>>> submitted = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                submitted.add(
                        executor.submit(
                                () -> {
                                    startBarrier.await();
                                    return manager.getOrDiscoverOwner();
                                }));
            }

            List<CompletableFuture<ServerNode>> discoveries = new ArrayList<>();
            for (Future<CompletableFuture<ServerNode>> future : submitted) {
                discoveries.add(future.get());
            }

            // All callers must observe the same in-flight request instead of sending their own
            // FindConsumerGroupOwner request.
            assertThat(discoveries.stream().distinct().count()).isEqualTo(1);
            rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));
            for (CompletableFuture<ServerNode> discovery : discoveries) {
                assertOwner(discovery.get(), TestingMetadataUpdater.NODE2);
            }
            assertThat(requests).hasValue(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDiscoveryFailsWhenNoTabletServerIsKnown() {
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Collections.emptyList(),
                        Collections.<TablePath, TableInfo>emptyMap(),
                        Collections.emptyMap(),
                        new Configuration());
        ConsumerGroupManager manager = new ConsumerGroupManager("group-1", metadataUpdater);

        assertThatThrownBy(() -> manager.getOrDiscoverOwner().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDecodeFailureIsNotWrappedInCompletionException() {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        // The response violates the owner contract, so decoding must fail.
                        return CompletableFuture.completedFuture(
                                new FindConsumerGroupOwnerResponse());
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        // Async composition must observe the declared failure type instead of the
        // CompletionException wrapper that thenApply adds around decode errors.
        assertThat(error.get()).isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDecodeFailureOnIncompleteOwner() {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        // The owner is present but its host and port are missing.
                        return CompletableFuture.completedFuture(
                                new FindConsumerGroupOwnerResponse()
                                        .setOwner(new PbServerNode().setNodeId(1)));
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        assertThat(error.get())
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class)
                .hasMessageContaining("incomplete owner");
    }

    @Test
    void testCommitRequestIsEncodedInStableOrder() {
        AtomicReference<CommitConsumerOffsetsRequest> captured = new AtomicReference<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            captured.set(request);
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        Map<TableBucket, OffsetMetadata> offsets = new LinkedHashMap<>();
        offsets.put(new TableBucket(2L, null, 3), new OffsetMetadata(23L, "second"));
        offsets.put(new TableBucket(1L, 5L, 2), new OffsetMetadata(12L, "partitioned"));
        offsets.put(new TableBucket(1L, null, 1), new OffsetMetadata(11L));

        AtomicReference<Exception> callbackError = new AtomicReference<>();
        manager.commitOffsetsAsync(offsets, (ignored, error) -> callbackError.set(error));
        manager.invokeCompletedOffsetCommitCallbacks();

        CommitConsumerOffsetsRequest request = captured.get();
        assertThat(request.getGroupId()).isEqualTo("group-1");
        assertThat(request.getTablesList()).hasSize(2);
        assertThat(request.getTablesList().get(0).getTableId()).isEqualTo(1L);
        assertThat(request.getTablesList().get(0).getBucketsList())
                .extracting(bucket -> bucket.getBucketId())
                .containsExactly(1, 2);
        assertThat(request.getTablesList().get(0).getBucketsList().get(1).getPartitionId())
                .isEqualTo(5L);
        assertThat(request.getTablesList().get(1).getTableId()).isEqualTo(2L);
        assertThat(callbackError.get()).isNull();
    }

    @Test
    void testCommitAsyncReportsBucketErrorAndForgetsOwner() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CommitConsumerOffsetsResponse response =
                errorResponse(Errors.NOT_CONSUMER_GROUP_OWNER, "owner moved");
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            discoveries.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ownerResponse(TestingMetadataUpdater.NODE2));
                        },
                        request -> CompletableFuture.completedFuture(response));
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        AtomicReference<Exception> callbackError = new AtomicReference<>();
        manager.commitOffsetsAsync(sampleOffsets(), (ignored, error) -> callbackError.set(error));
        manager.invokeCompletedOffsetCommitCallbacks();

        assertThat(callbackError.get()).isInstanceOf(NotConsumerGroupOwnerException.class);
        manager.getOrDiscoverOwner().get();
        assertThat(discoveries).hasValue(2);
    }

    @Test
    void testCommitAsyncCallbackIsInvokedOnceWithImmutableSnapshot() {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> response);
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));
        Map<TableBucket, OffsetMetadata> offsets = sampleOffsets();
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<Map<TableBucket, OffsetMetadata>> callbackOffsets = new AtomicReference<>();

        manager.commitOffsetsAsync(
                offsets,
                (committed, error) -> {
                    callbacks.incrementAndGet();
                    callbackOffsets.set(committed);
                });
        offsets.clear();
        response.complete(new CommitConsumerOffsetsResponse());
        manager.invokeCompletedOffsetCommitCallbacks();
        manager.invokeCompletedOffsetCommitCallbacks();

        assertThat(callbacks).hasValue(1);
        assertThat(callbackOffsets.get()).hasSize(1);
        assertThatThrownBy(() -> callbackOffsets.get().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEmptyCommitCompletesWithoutRpc() {
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            commits.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));
        AtomicInteger callbacks = new AtomicInteger();

        manager.commitOffsetsAsync(
                Collections.emptyMap(), (offsets, error) -> callbacks.incrementAndGet());
        manager.invokeCompletedOffsetCommitCallbacks();

        assertThat(callbacks).hasValue(1);
        assertThat(commits).hasValue(0);
        assertThat(manager.commitOffsetsSync(Collections.emptyMap(), Time.SYSTEM.timer(1)))
                .isTrue();
    }

    @Test
    void testCommitSyncRetriesRetriableFailure() {
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            if (commits.getAndIncrement() == 0) {
                                return failedFuture(
                                        new ConsumerGroupOwnerNotAvailableException(
                                                "owner unavailable"));
                            }
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ManualTime time = new ManualTime();
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway), 10L);

        assertThat(manager.commitOffsetsSync(sampleOffsets(), time.timer(100L))).isTrue();
        assertThat(commits).hasValue(2);
        assertThat(time.milliseconds()).isEqualTo(10L);
    }

    private static Map<TableBucket, OffsetMetadata> sampleOffsets() {
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, null, 0), new OffsetMetadata(10L, "metadata"));
        return offsets;
    }

    private static CommitConsumerOffsetsResponse errorResponse(Errors error, String message) {
        return new CommitConsumerOffsetsResponse()
                .addAllTables(
                        Collections.singletonList(
                                new PbCommitConsumerOffsetsResponseTable()
                                        .setTableId(1L)
                                        .addAllBuckets(
                                                Collections.singletonList(
                                                        new PbCommitConsumerOffsetsResponseBucket()
                                                                .setBucketId(0)
                                                                .setErrorCode(error.code())
                                                                .setErrorMessage(message)))));
    }

    private static TestTabletServerGateway gateway(
            OwnerRequestHandler ownerHandler, CommitRequestHandler commitHandler) {
        return new TestTabletServerGateway(false, Collections.emptySet()) {
            @Override
            public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                    FindConsumerGroupOwnerRequest request) {
                return ownerHandler.handle(request);
            }

            @Override
            public CompletableFuture<CommitConsumerOffsetsResponse> commitConsumerOffsets(
                    CommitConsumerOffsetsRequest request) {
                return commitHandler.handle(request);
            }
        };
    }

    private static TestingMetadataUpdater metadataUpdater(TestTabletServerGateway gateway) {
        Map<Integer, TestTabletServerGateway> gateways = new HashMap<>();
        gateways.put(TestingMetadataUpdater.NODE1.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE2.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE3.id(), gateway);
        return new TestingMetadataUpdater(
                TestingMetadataUpdater.COORDINATOR,
                Arrays.asList(
                        TestingMetadataUpdater.NODE1,
                        TestingMetadataUpdater.NODE2,
                        TestingMetadataUpdater.NODE3),
                Collections.emptyMap(),
                gateways,
                new Configuration());
    }

    private static FindConsumerGroupOwnerResponse ownerResponse(ServerNode owner) {
        return new FindConsumerGroupOwnerResponse()
                .setOwner(
                        new PbServerNode()
                                .setNodeId(owner.id())
                                .setHost(owner.host())
                                .setPort(owner.port()));
    }

    private static void assertOwner(ServerNode actual, ServerNode expected) {
        assertThat(actual.id()).isEqualTo(expected.id());
        assertThat(actual.host()).isEqualTo(expected.host());
        assertThat(actual.port()).isEqualTo(expected.port());
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    private interface OwnerRequestHandler {
        CompletableFuture<FindConsumerGroupOwnerResponse> handle(
                FindConsumerGroupOwnerRequest request);
    }

    private interface CommitRequestHandler {
        CompletableFuture<CommitConsumerOffsetsResponse> handle(
                CommitConsumerOffsetsRequest request);
    }

    private static final class ManualTime implements Time {
        private long nowMs;

        @Override
        public long milliseconds() {
            return nowMs;
        }

        @Override
        public void sleep(long ms) {
            nowMs += ms;
        }
    }
}
