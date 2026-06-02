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
package org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.streaming.connectors.gcp.pubsub.proto.PubSubEnumeratorCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.RateLimitChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.SubscriberSettingsChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.pubsub.v1.ProjectSubscriptionName;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SplitEnumerator} that assigns Pub/Sub {@link SubscriptionSplit}s to registered readers.
 *
 * <p>Original PR #32 behavior (preserved): each reader receives one split for the
 * source's configured subscription. All readers thus pull in parallel from the same subscription.
 *
 * <p>Extension for dynamic subscriptions (Plan #1 Phase C): callers may invoke
 * {@link #addSubscriptions(Set)} to register additional subscriptions at runtime. Each
 * dynamically-added subscription produces a single split that is hash-routed to one registered
 * reader; if the target reader is not yet registered, the split is parked in
 * {@code pendingSplitAssignments} and flushed when the reader joins.
 *
 * <p>{@link #updateRateLimits} and {@link #updateSubscriberSettings} broadcast {@link
 * RateLimitChangeEvent} and {@link SubscriberSettingsChangeEvent} to all registered readers.
 */
public class PubSubSplitEnumerator
        implements SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> {
    private final ProjectSubscriptionName subscriptionName;
    private final String projectName;
    private final SplitEnumeratorContext<SubscriptionSplit> context;
    private final HashMap<Integer, SubscriptionSplit> readersWithAssignments;

    /** Subscriptions added at runtime via {@link #addSubscriptions(Set)}. */
    private final Set<String> assignedSubscriptions = new HashSet<>();

    /** Splits awaiting assignment because their target reader is not yet registered. */
    private final Map<Integer, Set<SubscriptionSplit>> pendingSplitAssignments = new HashMap<>();

    public PubSubSplitEnumerator(
            ProjectSubscriptionName subscriptionName,
            SplitEnumeratorContext<SubscriptionSplit> context,
            HashMap<Integer, SubscriptionSplit> readersWithAssignments) {
        this.subscriptionName = subscriptionName;
        this.projectName = subscriptionName.getProject();
        this.context = context;
        this.readersWithAssignments = readersWithAssignments;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
        readersWithAssignments.remove(subtaskId);
        // It's possible that the reader has recovered already, so check if it needs a new
        // assignment.
        checkForUnassignedReaders();
    }

    @Override
    public void addReader(int subtaskId) {
        // First, flush any dynamically-added splits parked for this reader.
        Set<SubscriptionSplit> pending = pendingSplitAssignments.remove(subtaskId);
        if (pending != null && !pending.isEmpty()) {
            context.assignSplits(
                    new SplitsAssignment<>(
                            Collections.singletonMap(subtaskId, new ArrayList<>(pending))));
        }
        // Then perform the original PR-#32 assignment of the source-default subscription.
        checkForUnassignedReaders();
    }

    @Override
    public PubSubEnumeratorCheckpoint snapshotState(long checkpointId) {
        List<PubSubEnumeratorCheckpoint.Assignment> assignments = new ArrayList<>();
        readersWithAssignments.forEach(
                (task, split) -> {
                    assignments.add(
                            PubSubEnumeratorCheckpoint.Assignment.newBuilder()
                                    .setSplit(split.toProto())
                                    .setSubtask(task)
                                    .build());
                });
        return PubSubEnumeratorCheckpoint.newBuilder().addAllAssignments(assignments).build();
    }

    @Override
    public void close() {}

    private void checkForUnassignedReaders() {
        // Remove all assignments for readers that are no longer registered.
        Set<Integer> registeredReaders = context.registeredReaders().keySet();
        readersWithAssignments.keySet().removeIf(task -> !registeredReaders.contains(task));

        // For all readers without an assignment, assign a new Split.
        HashMap<Integer, List<SubscriptionSplit>> newAssignments = new HashMap<>();
        for (Integer reader : registeredReaders) {
            if (!readersWithAssignments.containsKey(reader)) {
                SubscriptionSplit newSplit =
                        SubscriptionSplit.create(subscriptionName, Integer.toString(reader));
                readersWithAssignments.put(reader, newSplit);
                newAssignments.put(reader, Collections.singletonList(newSplit));
            }
        }
        if (!newAssignments.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(newAssignments));
        }
    }

    /**
     * Add the given subscription names as new splits. Idempotent — already-added subscriptions
     * are skipped. Each new subscription becomes one {@link SubscriptionSplit} that is
     * hash-routed to a single registered reader; if that reader is not yet registered, the
     * split is parked and flushed when {@link #addReader(int)} fires for it.
     *
     * <p>Note: this complements the source-default subscription assigned by
     * {@link #checkForUnassignedReaders()}, which fans out across all readers. Dynamically-added
     * subscriptions are assigned to a single reader each.
     */
    public synchronized void addSubscriptions(Set<String> newSubscriptions) {
        if (newSubscriptions == null || newSubscriptions.isEmpty()) {
            return;
        }
        Set<String> reallyNew = new HashSet<>(newSubscriptions);
        reallyNew.removeAll(assignedSubscriptions);
        // Also skip the source-default subscription — it's already assigned via the original path.
        reallyNew.remove(subscriptionName.getSubscription());
        if (reallyNew.isEmpty()) {
            return;
        }

        int parallelism = Math.max(1, context.currentParallelism());
        Map<Integer, List<SubscriptionSplit>> assignment = new LinkedHashMap<>();
        for (String sub : reallyNew) {
            ProjectSubscriptionName projectSub = ProjectSubscriptionName.of(projectName, sub);
            SubscriptionSplit split = SubscriptionSplit.create(projectSub);
            int reader = Math.floorMod(split.splitId().hashCode(), parallelism);
            if (context.registeredReaders().containsKey(reader)) {
                assignment.computeIfAbsent(reader, r -> new ArrayList<>()).add(split);
            } else {
                pendingSplitAssignments
                        .computeIfAbsent(reader, r -> new HashSet<>())
                        .add(split);
            }
            assignedSubscriptions.add(sub);
        }
        if (!assignment.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(assignment));
        }
    }

    /**
     * Update rate limits by broadcasting a {@link RateLimitChangeEvent} to all registered readers.
     *
     * @param newSourceDefault new source-level default rate limit (null = unchanged)
     * @param perSubOverrides per-subscription overrides; a {@code null} value in this map clears
     *     the override (revert to source-level default)
     */
    public synchronized void updateRateLimits(
            Long newSourceDefault, Map<String, Long> perSubOverrides) {
        RateLimitChangeEvent ev = new RateLimitChangeEvent(newSourceDefault, perSubOverrides);
        broadcastEvent(ev);
    }

    /**
     * Update subscriber gRPC settings by broadcasting a {@link SubscriberSettingsChangeEvent} to
     * all registered readers. Null fields mean "unchanged".
     */
    public synchronized void updateSubscriberSettings(
            Integer requestTimeoutSec, Integer requestRetries) {
        SubscriberSettingsChangeEvent ev =
                new SubscriberSettingsChangeEvent(requestTimeoutSec, requestRetries);
        broadcastEvent(ev);
    }

    /** Returns the set of subscriptions added via {@link #addSubscriptions(Set)}. */
    public synchronized Set<String> getAssignedSubscriptions() {
        return Collections.unmodifiableSet(new HashSet<>(assignedSubscriptions));
    }

    private void broadcastEvent(SourceEvent ev) {
        for (Integer subtask : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(subtask, ev);
        }
    }
}
