/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Range;

/**
 * This is a consumer selector using consistent hashing to evenly split
 * the number of keys assigned to each consumer.
 */
@Slf4j
public class ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    // use NUL character as field separator for hash key calculation
    private static final String KEY_SEPARATOR = "\0";
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Consistent-Hash ring
    private final NavigableMap<Integer, ConsumerIdentityWrapper> hashRing;
    // Tracks the used consumer name indexes for each consumer name
    private final ConsumerNameIndexTracker consumerNameIndexTracker = new ConsumerNameIndexTracker();

    private final int numberOfPoints;
    private final Range keyHashRange;
    private final boolean addOrRemoveReturnsImpactedConsumersResult;
    private ConsumerHashAssignmentsSnapshot consumerHashAssignmentsSnapshot;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this(numberOfPoints, false);
    }

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints,
                                                      boolean addOrRemoveReturnsImpactedConsumersResult) {
        this(numberOfPoints, addOrRemoveReturnsImpactedConsumersResult, DEFAULT_RANGE_SIZE - 1);
    }

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints,
                                                      boolean addOrRemoveReturnsImpactedConsumersResult,
                                                      int rangeMaxValue) {
        this.addOrRemoveReturnsImpactedConsumersResult = addOrRemoveReturnsImpactedConsumersResult;
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
        this.keyHashRange = Range.of(STICKY_KEY_HASH_NOT_SET + 1, rangeMaxValue);
        this.consumerHashAssignmentsSnapshot = addOrRemoveReturnsImpactedConsumersResult
                ? ConsumerHashAssignmentsSnapshot.empty()
                : null;
    }

    @Override
    public CompletableFuture<Optional<ImpactedConsumersResult>> addConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            int hashPointsAdded = 0;
            int hashPointCollisions = 0;
            for (int i = 0; i < numberOfPoints; i++) {
                int consumerNameIndex =
                        consumerNameIndexTracker.increaseConsumerRefCountAndReturnIndex(consumerIdentityWrapper);
                int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                // When there's a collision, the entry won't be added to the hash ring.
                // This isn't a problem with the consumerNameIndexTracker solution since the collisions won't align
                // for all hash ring points when using the same consumer name. This won't affect the overall
                // distribution significantly when the number of hash ring points is sufficiently large (>100).
                ConsumerIdentityWrapper existing = hashRing.putIfAbsent(hash, consumerIdentityWrapper);
                if (existing != null) {
                    hashPointCollisions++;
                    // reduce the ref count which was increased before adding since the consumer was not added
                    consumerNameIndexTracker.decreaseConsumerRefCount(consumerIdentityWrapper);
                } else {
                    hashPointsAdded++;
                }
            }
            if (hashPointsAdded == 0) {
                log.error("Failed to add consumer '{}' to the hash ring. There were {} collisions. Consider increasing "
                                + "the number of points ({}) per consumer by setting "
                                + "subscriptionKeySharedConsistentHashingReplicaPoints={}",
                        consumer, hashPointCollisions, numberOfPoints,
                        Math.max((int) (numberOfPoints * 1.5d), numberOfPoints + 1));
            }
            if (log.isDebugEnabled()) {
                log.debug("Added consumer '{}' with {} points, {} collisions", consumer, hashPointsAdded,
                        hashPointCollisions);
            }
            if (!addOrRemoveReturnsImpactedConsumersResult) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
            ImpactedConsumersResult impactedConsumers =
                    consumerHashAssignmentsSnapshot.resolveImpactedConsumers(assignmentsAfter);
            consumerHashAssignmentsSnapshot = assignmentsAfter;
            return CompletableFuture.completedFuture(Optional.of(impactedConsumers));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Calculate the hash for a consumer and hash ring point.
     * The hash is calculated based on the consumer name, consumer name index, and hash ring point index.
     * The resulting hash is used as the key to insert the consumer into the hash ring.
     *
     * @param consumer the consumer
     * @param consumerNameIndex the index of the consumer name
     * @param hashRingPointIndex the index of the hash ring point
     * @return the hash value
     */
    private int calculateHashForConsumerAndIndex(Consumer consumer, int consumerNameIndex,
                                                        int hashRingPointIndex) {
        String key = consumer.consumerName() + KEY_SEPARATOR + consumerNameIndex + KEY_SEPARATOR + hashRingPointIndex;
        return makeStickyKeyHash(key.getBytes());
    }

    @Override
    public Optional<ImpactedConsumersResult> removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            int consumerNameIndex = consumerNameIndexTracker.getTrackedIndex(consumerIdentityWrapper);
            if (consumerNameIndex > -1) {
                // Remove all the points that were added for this consumer
                for (int i = 0; i < numberOfPoints; i++) {
                    int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                    if (hashRing.remove(hash, consumerIdentityWrapper)) {
                        consumerNameIndexTracker.decreaseConsumerRefCount(consumerIdentityWrapper);
                    }
                }
            }
            if (!addOrRemoveReturnsImpactedConsumersResult) {
                return Optional.empty();
            }
            ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
            ImpactedConsumersResult impactedConsumers =
                    consumerHashAssignmentsSnapshot.resolveImpactedConsumers(assignmentsAfter);
            consumerHashAssignmentsSnapshot = assignmentsAfter;
            return Optional.of(impactedConsumers);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Consumer select(int hash) {
        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return null;
            }
            Map.Entry<Integer, ConsumerIdentityWrapper> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                return ceilingEntry.getValue().consumer;
            } else {
                // Handle wrap-around in the hash ring, return the first consumer
                return hashRing.firstEntry().getValue().consumer;
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Range getKeyHashRange() {
        return keyHashRange;
    }

    @Override
    public ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        rwLock.readLock().lock();
        try {
            return consumerHashAssignmentsSnapshot != null ? consumerHashAssignmentsSnapshot
                    : internalGetConsumerHashAssignmentsSnapshot();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private ConsumerHashAssignmentsSnapshot internalGetConsumerHashAssignmentsSnapshot() {
        if (hashRing.isEmpty()) {
            return ConsumerHashAssignmentsSnapshot.empty();
        }
        List<HashRangeAssignment> result = new ArrayList<>();
        int start = getKeyHashRange().getStart();
        int lastKey = -1;
        Consumer previousConsumer = null;
        Range previousRange = null;
        for (Map.Entry<Integer, ConsumerIdentityWrapper> entry: hashRing.entrySet()) {
            Consumer consumer = entry.getValue().consumer;
            Range range;
            if (consumer == previousConsumer) {
                // join ranges
                result.remove(result.size() - 1);
                range = Range.of(previousRange.getStart(), entry.getKey());
            } else {
                range = Range.of(start, entry.getKey());
            }
            result.add(new HashRangeAssignment(range, consumer));
            lastKey = entry.getKey();
            start = lastKey + 1;
            previousConsumer = consumer;
            previousRange = range;
        }
        // Handle wrap-around
        Consumer firstConsumer = hashRing.firstEntry().getValue().consumer;
        if (lastKey != getKeyHashRange().getEnd()) {
            Range range;
            if (firstConsumer == previousConsumer && previousRange.getEnd() == lastKey) {
                // join ranges
                result.remove(result.size() - 1);
                range = Range.of(previousRange.getStart(), getKeyHashRange().getEnd());
            } else {
                range = Range.of(lastKey + 1, getKeyHashRange().getEnd());
            }
            result.add(new HashRangeAssignment(range, firstConsumer));
        }
        return ConsumerHashAssignmentsSnapshot.of(result);
    }
}
