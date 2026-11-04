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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.log.internals.utils.Time;
import org.apache.fluss.client.table.scanner.log.internals.utils.Timer;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Discovers and caches the tablet server that owns a consumer group, and commits consumer offsets
 * to that owner.
 *
 * <p>Discovery is initiated by scanner threads while RPC completion callbacks run on network
 * threads; both coordinate through this object's monitor, mirroring how LogFetcher shares state
 * between the scanner thread and fetch-response callbacks. Owner state is guarded by {@code this},
 * and the monitor is never held across blocking waits. Completed asynchronous offset-commit
 * callbacks are stored in a concurrent queue so they can be invoked by scanner threads without
 * holding that monitor.
 */
@Internal
public final class ConsumerGroupManager implements AutoCloseable {

    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;

    private static final Comparator<Map.Entry<TableBucket, OffsetMetadata>> OFFSET_ORDER =
            Comparator.comparingLong(
                            (Map.Entry<TableBucket, OffsetMetadata> entry) ->
                                    entry.getKey().getTableId())
                    .thenComparing(
                            entry -> entry.getKey().getPartitionId(),
                            Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparingInt(entry -> entry.getKey().getBucket());

    private static final Comparator<TableBucket> TABLE_BUCKET_ORDER =
            Comparator.comparingLong(TableBucket::getTableId)
                    .thenComparing(
                            TableBucket::getPartitionId,
                            Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparingInt(TableBucket::getBucket);

    private final String groupId;
    private final MetadataUpdater metadataUpdater;
    private final Time time;
    private final long retryBackoffMs;
    private final boolean autoCommitEnabled;
    private final long autoCommitIntervalMs;

    /** The cached owner tablet server; null while the owner is unknown. */
    private ServerNode ownerNode;

    /** The pending owner-discovery request; null while no discovery is in flight. */
    private CompletableFuture<ServerNode> ownerDiscovery;

    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits =
            new ConcurrentLinkedQueue<>();
    private final AtomicReference<PendingCommittedOffsetRequest> pendingOffsetFetch =
            new AtomicReference<>();
    private final Timer autoCommitTimer;

    /**
     * Creates a manager that discovers and caches the owner of one consumer group.
     *
     * @param groupId the consumer group whose owner is discovered
     * @param metadataUpdater the client metadata used to pick a tablet server for discovery and to
     *     open gateways to the discovered owner
     */
    public ConsumerGroupManager(String groupId, MetadataUpdater metadataUpdater) {
        this(groupId, metadataUpdater, Time.SYSTEM, DEFAULT_RETRY_BACKOFF_MS, false, 0L);
    }

    public ConsumerGroupManager(
            String groupId,
            MetadataUpdater metadataUpdater,
            boolean autoCommitEnabled,
            long autoCommitIntervalMs) {
        this(
                groupId,
                metadataUpdater,
                Time.SYSTEM,
                DEFAULT_RETRY_BACKOFF_MS,
                autoCommitEnabled,
                autoCommitIntervalMs);
    }

    ConsumerGroupManager(
            String groupId, MetadataUpdater metadataUpdater, Time time, long retryBackoffMs) {
        this(groupId, metadataUpdater, time, retryBackoffMs, false, 0L);
    }

    ConsumerGroupManager(
            String groupId,
            MetadataUpdater metadataUpdater,
            Time time,
            long retryBackoffMs,
            boolean autoCommitEnabled,
            long autoCommitIntervalMs) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.metadataUpdater =
                Objects.requireNonNull(metadataUpdater, "metadataUpdater must not be null");
        this.time = Objects.requireNonNull(time, "time must not be null");
        if (retryBackoffMs < 0) {
            throw new IllegalArgumentException("retryBackoffMs must not be negative");
        }
        this.retryBackoffMs = retryBackoffMs;
        if (autoCommitEnabled && autoCommitIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "autoCommitIntervalMs must be positive when auto-commit is enabled");
        }
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.autoCommitTimer = autoCommitEnabled ? time.timer(autoCommitIntervalMs) : null;
    }

    /** Returns the cached owner or shares the single in-flight owner-discovery request. */
    public synchronized CompletableFuture<ServerNode> getOrDiscoverOwner() {
        if (ownerNode != null
                && metadataUpdater.newTabletServerClientForNode(ownerNode.id()) != null) {
            return CompletableFuture.completedFuture(ownerNode);
        }
        if (ownerDiscovery != null) {
            return ownerDiscovery;
        }

        // thenApply wraps decode errors in a CompletionException; unwrap them so async
        // composition observes the declared retriable failure type directly. Blocking callers
        // are unaffected either way because CompletableFuture.get() removes that wrapper.
        CompletableFuture<ServerNode> created = unwrapFailure(discoverOwner());

        // Publish the future before attaching the completion callback: when discovery fails
        // synchronously, the callback runs on this calling thread and relies on seeing the
        // published future so it can clear the slot for the next attempt.
        ownerDiscovery = created;
        created.whenComplete(
                (owner, error) -> {
                    synchronized (this) {
                        if (error == null) {
                            ownerNode = owner;
                        }
                        if (ownerDiscovery == created) {
                            ownerDiscovery = null;
                        }
                    }
                });
        return created;
    }

    /** Clears a stale owner so that the next operation performs owner discovery again. */
    public synchronized void markOwnerUnknown() {
        ownerNode = null;
    }

    /** Returns a gateway for the cached owner, or null when no usable owner is cached. */
    public synchronized TabletServerGateway cachedOwnerGateway() {
        if (ownerNode == null) {
            return null;
        }
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(ownerNode.id());
        if (gateway == null) {
            markOwnerUnknown();
        }
        return gateway;
    }

    /** Sends an asynchronous offset commit and queues its callback for caller-thread execution. */
    public void commitOffsetsAsync(
            Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();
        Map<TableBucket, OffsetMetadata> offsetsCopy = immutableOffsets(offsets);
        sendCommit(offsetsCopy)
                .whenComplete(
                        (ignored, error) ->
                                completedOffsetCommits.add(
                                        new OffsetCommitCompletion(
                                                offsetsCopy, callback, asException(error))));
    }

    /** Commits offsets, retrying retriable failures until the supplied timer expires. */
    public boolean commitOffsetsSync(Map<TableBucket, OffsetMetadata> offsets, Timer timer) {
        invokeCompletedOffsetCommitCallbacks();
        Map<TableBucket, OffsetMetadata> offsetsCopy = immutableOffsets(offsets);
        Objects.requireNonNull(timer, "timer must not be null");
        if (offsetsCopy.isEmpty()) {
            return true;
        }

        while (timer.notExpired()) {
            try {
                sendCommit(offsetsCopy)
                        .get(Math.max(1L, timer.remainingMs()), TimeUnit.MILLISECONDS);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while committing consumer offsets", e);
            } catch (TimeoutException e) {
                return false;
            } catch (ExecutionException | CancellationException e) {
                Throwable cause = unwrap(e);
                if (!(cause instanceof RetriableException)) {
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new FlussRuntimeException("Failed to commit consumer offsets", cause);
                }
                timer.update();
                if (timer.notExpired()) {
                    timer.sleep(retryBackoffMs);
                }
            }
        }
        return false;
    }

    /** Invokes completed asynchronous commit callbacks on the calling thread. */
    void invokeCompletedOffsetCommitCallbacks() {
        OffsetCommitCompletion completion;
        while ((completion = completedOffsetCommits.poll()) != null) {
            completion.invoke();
        }
    }

    /** Fetches committed offsets, retrying retriable failures until the timer expires. */
    public Map<TableBucket, OffsetMetadata> fetchCommittedOffsets(
            Set<TableBucket> tableBuckets, Timer timer) {
        Set<TableBucket> requestedBuckets = immutableTableBuckets(tableBuckets);
        Objects.requireNonNull(timer, "timer must not be null");
        if (requestedBuckets.isEmpty()) {
            return Collections.emptyMap();
        }

        while (timer.notExpired()) {
            PendingCommittedOffsetRequest pending = getOrCreateOffsetFetch(requestedBuckets);
            try {
                Map<TableBucket, OffsetMetadata> offsets =
                        pending.response.get(
                                Math.max(1L, timer.remainingMs()), TimeUnit.MILLISECONDS);
                pendingOffsetFetch.compareAndSet(pending, null);
                return offsets;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(
                        "Interrupted while fetching committed consumer offsets", e);
            } catch (TimeoutException e) {
                return null;
            } catch (ExecutionException | CancellationException e) {
                pendingOffsetFetch.compareAndSet(pending, null);
                Throwable cause = unwrap(e);
                if (!(cause instanceof RetriableException)) {
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new FlussRuntimeException(
                            "Failed to fetch committed consumer offsets", cause);
                }
                timer.update();
                if (timer.notExpired()) {
                    timer.sleep(retryBackoffMs);
                }
            }
        }
        return null;
    }

    /** Performs an asynchronous auto-commit when its interval has elapsed. */
    public boolean maybeAutoCommitOffsetsAsync(
            long nowMs, Map<TableBucket, OffsetMetadata> offsets) {
        invokeCompletedOffsetCommitCallbacks();
        if (!autoCommitEnabled) {
            return false;
        }
        autoCommitTimer.update(nowMs);
        if (!autoCommitTimer.isExpired()) {
            return false;
        }
        autoCommitTimer.reset(autoCommitIntervalMs);
        commitOffsetsAsync(offsets, null);
        return true;
    }

    /** Performs the final synchronous auto-commit used by scanner shutdown. */
    public boolean maybeAutoCommitOffsetsSync(
            Map<TableBucket, OffsetMetadata> offsets, Timer timer) {
        return !autoCommitEnabled || commitOffsetsSync(offsets, timer);
    }

    private synchronized PendingCommittedOffsetRequest getOrCreateOffsetFetch(
            Set<TableBucket> requestedBuckets) {
        PendingCommittedOffsetRequest current = pendingOffsetFetch.get();
        if (current != null && current.requestedBuckets.equals(requestedBuckets)) {
            return current;
        }

        PendingCommittedOffsetRequest created =
                new PendingCommittedOffsetRequest(
                        requestedBuckets, sendGetConsumerOffsetsRequest(requestedBuckets));
        pendingOffsetFetch.set(created);
        return created;
    }

    private CompletableFuture<Map<TableBucket, OffsetMetadata>> sendGetConsumerOffsetsRequest(
            Set<TableBucket> tableBuckets) {
        GetConsumerOffsetsRequest request = buildGetConsumerOffsetsRequest(tableBuckets);
        return getOrDiscoverOwner()
                .thenCompose(
                        owner -> {
                            TabletServerGateway gateway =
                                    metadataUpdater.newTabletServerClientForNode(owner.id());
                            if (gateway == null) {
                                markOwnerUnknown();
                                return ConsumerGroupManager
                                        .<GetConsumerOffsetsResponse>failedFuture(
                                                new ConsumerGroupOwnerNotAvailableException(
                                                        "No gateway is available for consumer group owner "
                                                                + owner.id()));
                            }
                            return gateway.getConsumerOffsets(request);
                        })
                .thenApply(this::decodeGetConsumerOffsetsResponse)
                .whenComplete(
                        (ignored, error) -> {
                            if (shouldForgetOwner(unwrap(error))) {
                                markOwnerUnknown();
                            }
                        });
    }

    private GetConsumerOffsetsRequest buildGetConsumerOffsetsRequest(
            Set<TableBucket> tableBuckets) {
        List<TableBucket> orderedBuckets = new ArrayList<>(tableBuckets);
        orderedBuckets.sort(TABLE_BUCKET_ORDER);
        Map<Long, List<PbGetConsumerOffsetsRequestBucket>> bucketsByTable = new LinkedHashMap<>();
        for (TableBucket tableBucket : orderedBuckets) {
            PbGetConsumerOffsetsRequestBucket bucket =
                    new PbGetConsumerOffsetsRequestBucket().setBucketId(tableBucket.getBucket());
            if (tableBucket.getPartitionId() != null) {
                bucket.setPartitionId(tableBucket.getPartitionId());
            }
            bucketsByTable
                    .computeIfAbsent(tableBucket.getTableId(), ignored -> new ArrayList<>())
                    .add(bucket);
        }

        GetConsumerOffsetsRequest request =
                new GetConsumerOffsetsRequest().setGroupId(groupId).setAllOffsets(false);
        for (Map.Entry<Long, List<PbGetConsumerOffsetsRequestBucket>> entry :
                bucketsByTable.entrySet()) {
            request.addAllTables(
                    Collections.singletonList(
                            new PbGetConsumerOffsetsRequestTable()
                                    .setTableId(entry.getKey())
                                    .addAllBuckets(entry.getValue())));
        }
        return request;
    }

    private Map<TableBucket, OffsetMetadata> decodeGetConsumerOffsetsResponse(
            GetConsumerOffsetsResponse response) {
        if (response == null) {
            throw new FlussRuntimeException("GetConsumerOffsets returned a null response");
        }
        Map<TableBucket, OffsetMetadata> offsets = new LinkedHashMap<>();
        for (PbGetConsumerOffsetsResponseTable table : response.getTablesList()) {
            for (PbGetConsumerOffsetsResponseBucket bucket : table.getBucketsList()) {
                Errors error =
                        bucket.hasErrorCode() ? Errors.forCode(bucket.getErrorCode()) : Errors.NONE;
                if (error != Errors.NONE) {
                    throw error.exception(
                            bucket.hasErrorMessage() ? bucket.getErrorMessage() : null);
                }
                if (bucket.hasOffset() && bucket.getOffset() >= 0) {
                    offsets.put(
                            new TableBucket(
                                    table.getTableId(),
                                    bucket.hasPartitionId() ? bucket.getPartitionId() : null,
                                    bucket.getBucketId()),
                            new OffsetMetadata(
                                    bucket.getOffset(),
                                    bucket.hasMetadata() ? bucket.getMetadata() : ""));
                }
            }
        }
        return Collections.unmodifiableMap(offsets);
    }

    private CompletableFuture<Void> sendCommit(Map<TableBucket, OffsetMetadata> offsets) {
        if (offsets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return getOrDiscoverOwner()
                .thenCompose(
                        owner -> {
                            TabletServerGateway gateway =
                                    metadataUpdater.newTabletServerClientForNode(owner.id());
                            if (gateway == null) {
                                markOwnerUnknown();
                                return ConsumerGroupManager
                                        .<CommitConsumerOffsetsResponse>failedFuture(
                                                new ConsumerGroupOwnerNotAvailableException(
                                                        "No gateway is available for consumer group owner "
                                                                + owner.id()));
                            }
                            return gateway.commitConsumerOffsets(buildCommitRequest(offsets));
                        })
                .thenApply(
                        response -> {
                            RuntimeException error = decodeCommitError(response);
                            if (error != null) {
                                throw new CompletionException(error);
                            }
                            return (Void) null;
                        })
                .whenComplete(
                        (ignored, error) -> {
                            if (shouldForgetOwner(unwrap(error))) {
                                markOwnerUnknown();
                            }
                        });
    }

    private CommitConsumerOffsetsRequest buildCommitRequest(
            Map<TableBucket, OffsetMetadata> offsets) {
        List<Map.Entry<TableBucket, OffsetMetadata>> entries = new ArrayList<>(offsets.entrySet());
        entries.sort(OFFSET_ORDER);

        Map<Long, List<PbCommitConsumerOffsetsRequestBucket>> bucketsByTable =
                new LinkedHashMap<>();
        for (Map.Entry<TableBucket, OffsetMetadata> entry : entries) {
            TableBucket tableBucket = entry.getKey();
            OffsetMetadata offsetMetadata = entry.getValue();
            PbCommitConsumerOffsetsRequestBucket bucket =
                    new PbCommitConsumerOffsetsRequestBucket()
                            .setBucketId(tableBucket.getBucket())
                            .setOffset(offsetMetadata.getOffset())
                            .setMetadata(offsetMetadata.getMetadata());
            if (tableBucket.getPartitionId() != null) {
                bucket.setPartitionId(tableBucket.getPartitionId());
            }
            bucketsByTable
                    .computeIfAbsent(tableBucket.getTableId(), ignored -> new ArrayList<>())
                    .add(bucket);
        }

        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(groupId);
        for (Map.Entry<Long, List<PbCommitConsumerOffsetsRequestBucket>> entry :
                bucketsByTable.entrySet()) {
            request.addAllTables(
                    Collections.singletonList(
                            new PbCommitConsumerOffsetsRequestTable()
                                    .setTableId(entry.getKey())
                                    .addAllBuckets(entry.getValue())));
        }
        return request;
    }

    private RuntimeException decodeCommitError(CommitConsumerOffsetsResponse response) {
        if (response == null) {
            return new FlussRuntimeException("CommitConsumerOffsets returned a null response");
        }
        for (PbCommitConsumerOffsetsResponseTable table : response.getTablesList()) {
            for (PbCommitConsumerOffsetsResponseBucket bucket : table.getBucketsList()) {
                Errors error =
                        bucket.hasErrorCode() ? Errors.forCode(bucket.getErrorCode()) : Errors.NONE;
                if (error != Errors.NONE) {
                    return error.exception(
                            bucket.hasErrorMessage() ? bucket.getErrorMessage() : null);
                }
            }
        }
        return null;
    }

    private CompletableFuture<ServerNode> discoverOwner() {
        ServerNode anyTabletServer = metadataUpdater.getRandomTabletServer();
        if (anyTabletServer == null) {
            return failedFuture(
                    new ConsumerGroupOwnerNotAvailableException(
                            "No tablet server is currently known to the client for group "
                                    + groupId));
        }

        TabletServerGateway gateway =
                metadataUpdater.newTabletServerClientForNode(anyTabletServer.id());
        if (gateway == null) {
            return failedFuture(
                    new ConsumerGroupOwnerNotAvailableException(
                            "No reachable tablet server gateway when discovering the owner of "
                                    + "group "
                                    + groupId));
        }

        FindConsumerGroupOwnerRequest request =
                new FindConsumerGroupOwnerRequest().setGroupId(groupId);
        return gateway.findConsumerGroupOwner(request).thenApply(ConsumerGroupManager::decodeOwner);
    }

    private static ServerNode decodeOwner(FindConsumerGroupOwnerResponse response) {
        if (response == null || !response.hasOwner()) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "FindConsumerGroupOwner response does not contain an owner");
        }
        PbServerNode owner = response.getOwner();
        if (!owner.hasHost() || !owner.hasPort()) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "FindConsumerGroupOwner response contains an incomplete owner");
        }
        return new ServerNode(
                owner.getNodeId(), owner.getHost(), owner.getPort(), ServerType.TABLET_SERVER);
    }

    private static Map<TableBucket, OffsetMetadata> immutableOffsets(
            Map<TableBucket, OffsetMetadata> offsets) {
        Objects.requireNonNull(offsets, "offsets must not be null");
        LinkedHashMap<TableBucket, OffsetMetadata> copy = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, OffsetMetadata> entry : offsets.entrySet()) {
            copy.put(
                    Objects.requireNonNull(entry.getKey(), "table bucket must not be null"),
                    Objects.requireNonNull(entry.getValue(), "offset metadata must not be null"));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Set<TableBucket> immutableTableBuckets(Set<TableBucket> tableBuckets) {
        Objects.requireNonNull(tableBuckets, "tableBuckets must not be null");
        java.util.LinkedHashSet<TableBucket> copy = new java.util.LinkedHashSet<>();
        for (TableBucket tableBucket : tableBuckets) {
            copy.add(Objects.requireNonNull(tableBucket, "table bucket must not be null"));
        }
        return Collections.unmodifiableSet(copy);
    }

    private static boolean shouldForgetOwner(Throwable error) {
        if (error == null) {
            return false;
        }
        Errors errors = Errors.forException(error);
        return errors == Errors.NOT_CONSUMER_GROUP_OWNER
                || errors == Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE
                || errors == Errors.CONSUMER_GROUP_OWNER_LOADING;
    }

    private static Exception asException(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause == null) {
            return null;
        }
        return cause instanceof Exception ? (Exception) cause : new FlussRuntimeException(cause);
    }

    /**
     * Returns a future whose failure is the actual exception instead of the CompletionException
     * wrapper added by dependent stages such as thenApply, so callers that compose asynchronously
     * observe the declared failure type directly.
     */
    private static <T> CompletableFuture<T> unwrapFailure(CompletableFuture<T> future) {
        CompletableFuture<T> unwrapped = new CompletableFuture<>();
        future.whenComplete(
                (value, error) -> {
                    if (error == null) {
                        unwrapped.complete(value);
                    } else {
                        unwrapped.completeExceptionally(unwrap(error));
                    }
                });
        return unwrapped;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    /** Invokes pending callbacks, forgets the owner, and cancels in-flight discovery. */
    @Override
    public void close() {
        invokeCompletedOffsetCommitCallbacks();

        CompletableFuture<ServerNode> discovery;
        synchronized (this) {
            ownerNode = null;
            discovery = ownerDiscovery;
            ownerDiscovery = null;
        }
        if (discovery != null) {
            // Waiting callers receive a CancellationException. The interrupt flag is false
            // because CompletableFuture cannot interrupt the underlying RPC.
            discovery.cancel(false);
        }
        PendingCommittedOffsetRequest pendingFetch = pendingOffsetFetch.getAndSet(null);
        if (pendingFetch != null) {
            pendingFetch.response.cancel(true);
        }
    }

    private static final class OffsetCommitCompletion {
        private final Map<TableBucket, OffsetMetadata> offsets;
        private final OffsetCommitCallback callback;
        private final Exception exception;

        private OffsetCommitCompletion(
                Map<TableBucket, OffsetMetadata> offsets,
                OffsetCommitCallback callback,
                Exception exception) {
            this.offsets = offsets;
            this.callback = callback;
            this.exception = exception;
        }

        private void invoke() {
            if (callback != null) {
                callback.onComplete(offsets, exception);
            }
        }
    }

    private static final class PendingCommittedOffsetRequest {
        private final Set<TableBucket> requestedBuckets;
        private final CompletableFuture<Map<TableBucket, OffsetMetadata>> response;

        private PendingCommittedOffsetRequest(
                Set<TableBucket> requestedBuckets,
                CompletableFuture<Map<TableBucket, OffsetMetadata>> response) {
            this.requestedBuckets = requestedBuckets;
            this.response = response;
        }
    }
}
