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
 * <p>Behavior is uniform across all subscriptions (Plan #1 Phase C addendum): every subscription —
 * whether supplied via the source builder at startup or added at runtime through {@link
 * #addSubscriptions(Set)} — becomes exactly one {@link SubscriptionSplit} that is hash-routed to a
 * single reader via {@code Math.floorMod(split.splitId().hashCode(), parallelism)}. There is no
 * asymmetric "primary" subscription that fans out to every reader.
 *
 * <p>Splits whose target reader is not yet registered are parked in {@code pendingSplitAssignments}
 * and flushed when {@link #addReader(int)} fires for the target subtask.
 *
 * <p>{@link #updateRateLimits} broadcasts a {@link RateLimitChangeEvent} to all registered readers.
 */
public class PubSubSplitEnumerator
        implements SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> {
    private final String projectName;
    private final SplitEnumeratorContext<SubscriptionSplit> context;

    /** All subscriptions the enumerator has ever produced a split for. Drives idempotency. */
    private final Set<String> assignedSubscriptions = new HashSet<>();

    /**
     * Splits currently held by readers, keyed by subtask. Restored from checkpoint and updated as
     * splits are assigned / returned via {@link #addSplitsBack(List, int)}.
     */
    private final Map<Integer, List<SubscriptionSplit>> readersWithAssignments = new HashMap<>();

    /** Splits awaiting assignment because their target reader is not yet registered. */
    private final Map<Integer, Set<SubscriptionSplit>> pendingSplitAssignments = new HashMap<>();

    /** Subscriptions present at construction time that have not yet been routed by {@link #start()}. */
    private final Set<String> initialSubscriptions;

    public PubSubSplitEnumerator(
            Set<String> initialSubscriptions,
            String projectName,
            SplitEnumeratorContext<SubscriptionSplit> context,
            PubSubEnumeratorCheckpoint checkpoint) {
        this.projectName = projectName;
        this.context = context;
        this.initialSubscriptions =
                initialSubscriptions == null
                        ? Collections.emptySet()
                        : new HashSet<>(initialSubscriptions);
        if (checkpoint != null) {
            this.assignedSubscriptions.addAll(checkpoint.getAssignedSubscriptionsList());
            for (PubSubEnumeratorCheckpoint.Assignment assignment :
                    checkpoint.getAssignmentsList()) {
                SubscriptionSplit split = SubscriptionSplit.fromProto(assignment.getSplit());
                readersWithAssignments
                        .computeIfAbsent(assignment.getSubtask(), k -> new ArrayList<>())
                        .add(split);
                // Defensive: ensure the split's subscription is tracked even on legacy checkpoints
                // that did not serialize assigned_subscriptions yet.
                assignedSubscriptions.add(split.subscriptionName().getSubscription());
            }
        }
    }

    @Override
    public void start() {
        // Route every initial subscription identically to dynamic additions: hash-route each one
        // to a single reader. Subscriptions already present in assignedSubscriptions (i.e. restored
        // from checkpoint) are skipped.
        if (!initialSubscriptions.isEmpty()) {
            addSubscriptions(initialSubscriptions);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
        readersWithAssignments.remove(subtaskId);
        if (splits == null || splits.isEmpty()) {
            return;
        }
        // Re-route returned splits to readers based on current parallelism, hash-keyed by split id.
        int parallelism = Math.max(1, context.currentParallelism());
        Map<Integer, List<SubscriptionSplit>> reassignment = new LinkedHashMap<>();
        for (SubscriptionSplit split : splits) {
            int reader = Math.floorMod(split.splitId().hashCode(), parallelism);
            if (context.registeredReaders().containsKey(reader)) {
                reassignment.computeIfAbsent(reader, r -> new ArrayList<>()).add(split);
                readersWithAssignments
                        .computeIfAbsent(reader, r -> new ArrayList<>())
                        .add(split);
            } else {
                pendingSplitAssignments
                        .computeIfAbsent(reader, r -> new HashSet<>())
                        .add(split);
            }
        }
        if (!reassignment.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(reassignment));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // Flush any splits parked for this reader (whether from initial start-up routing,
        // dynamic addSubscriptions calls, or addSplitsBack reassignments).
        Set<SubscriptionSplit> pending = pendingSplitAssignments.remove(subtaskId);
        if (pending != null && !pending.isEmpty()) {
            List<SubscriptionSplit> splits = new ArrayList<>(pending);
            context.assignSplits(
                    new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
            readersWithAssignments
                    .computeIfAbsent(subtaskId, k -> new ArrayList<>())
                    .addAll(splits);
        }
    }

    @Override
    public PubSubEnumeratorCheckpoint snapshotState(long checkpointId) {
        List<PubSubEnumeratorCheckpoint.Assignment> assignments = new ArrayList<>();
        readersWithAssignments.forEach(
                (task, splits) -> {
                    for (SubscriptionSplit split : splits) {
                        assignments.add(
                                PubSubEnumeratorCheckpoint.Assignment.newBuilder()
                                        .setSplit(split.toProto())
                                        .setSubtask(task)
                                        .build());
                    }
                });
        return PubSubEnumeratorCheckpoint.newBuilder()
                .addAllAssignments(assignments)
                .addAllAssignedSubscriptions(assignedSubscriptions)
                .build();
    }

    @Override
    public void close() {}

    /**
     * Add the given subscription names as new splits. Idempotent — subscriptions already known to
     * this enumerator (whether from initial start-up routing, a prior {@link #addSubscriptions}
     * call, or a restored checkpoint) are skipped. Each new subscription becomes one {@link
     * SubscriptionSplit} that is hash-routed to a single registered reader; if that reader is not
     * yet registered, the split is parked and flushed when {@link #addReader(int)} fires for it.
     */
    public synchronized void addSubscriptions(Set<String> newSubscriptions) {
        if (newSubscriptions == null || newSubscriptions.isEmpty()) {
            return;
        }
        Set<String> reallyNew = new HashSet<>(newSubscriptions);
        reallyNew.removeAll(assignedSubscriptions);
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
                readersWithAssignments
                        .computeIfAbsent(reader, r -> new ArrayList<>())
                        .add(split);
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

    /** Returns every subscription name the enumerator has produced a split for. */
    public synchronized Set<String> getAssignedSubscriptions() {
        return Collections.unmodifiableSet(new HashSet<>(assignedSubscriptions));
    }

    private void broadcastEvent(SourceEvent ev) {
        for (Integer subtask : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(subtask, ev);
        }
    }
}
