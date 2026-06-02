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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.streaming.connectors.gcp.pubsub.proto.PubSubEnumeratorCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PubSubSplitEnumerator} after the uniform-multi-subscription refactor
 * (Plan #1 Phase C addendum). Every subscription — initial or dynamically added — produces one
 * hash-routed split, so the original PR #32 "every reader gets a split for the source's
 * subscription" semantics no longer exist; tests cover the new uniform-routing behavior.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitEnumeratorTest {
    private static final String PROJECT = "project";
    private static final String SUBSCRIPTION = "subscription";

    @Mock SplitEnumeratorContext<SubscriptionSplit> mockContext;

    PubSubSplitEnumerator splitEnumerator;

    @Before
    public void doBeforeEachTest() {
        splitEnumerator =
                new PubSubSplitEnumerator(
                        Collections.singleton(SUBSCRIPTION),
                        PROJECT,
                        mockContext,
                        /* checkpoint= */ null);
    }

    private Map<Integer, ReaderInfo> createRegisteredReaders(List<Integer> readers) {
        Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
        for (Integer reader : readers) {
            registeredReaders.put(reader, new ReaderInfo(reader, "location"));
        }
        return registeredReaders;
    }

    private Set<String> assignedSubscriptionsFromCalls() {
        ArgumentCaptor<SplitsAssignment<SubscriptionSplit>> captor =
                ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(mockContext, atLeastOnce()).assignSplits(captor.capture());
        Set<String> seen = new HashSet<>();
        for (SplitsAssignment<SubscriptionSplit> sa : captor.getAllValues()) {
            for (List<SubscriptionSplit> splits : sa.assignment().values()) {
                for (SubscriptionSplit s : splits) {
                    seen.add(s.subscriptionName().getSubscription());
                }
            }
        }
        return seen;
    }

    @Test
    public void start_assignsInitialSubscriptionToHashRoutedReader() {
        // Register both readers so wherever the hash routes, an assignment will fire.
        when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0, 1)));
        when(mockContext.currentParallelism()).thenReturn(2);

        splitEnumerator.start();

        // Exactly one reader receives a split for the initial subscription.
        assertThat(assignedSubscriptionsFromCalls()).containsExactly(SUBSCRIPTION);
    }

    @Test
    public void addReader_flushesPendingSplitsForThatReader() {
        // Reader is not yet registered when start() routes the split — the split parks.
        // Sequenced stub: first call returns empty (parking), subsequent calls return both readers.
        when(mockContext.registeredReaders())
                .thenReturn(Collections.emptyMap())
                .thenReturn(createRegisteredReaders(Arrays.asList(0, 1)));
        when(mockContext.currentParallelism()).thenReturn(2);
        splitEnumerator.start();
        verify(mockContext, never()).assignSplits(any());

        // Now register both readers; whichever the hash points at receives the parked split.
        splitEnumerator.addReader(0);
        splitEnumerator.addReader(1);

        assertThat(assignedSubscriptionsFromCalls()).contains(SUBSCRIPTION);
    }

    @Test
    public void start_isIdempotentAcrossRepeatedAddSubscriptions() {
        when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));
        when(mockContext.currentParallelism()).thenReturn(1);

        splitEnumerator.start();
        // Re-adding the same initial subscription should be a no-op.
        splitEnumerator.addSubscriptions(Collections.singleton(SUBSCRIPTION));

        // Only one assignment was made for the subscription.
        ArgumentCaptor<SplitsAssignment<SubscriptionSplit>> captor =
                ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(mockContext, atLeastOnce()).assignSplits(captor.capture());
        int splitCount = 0;
        for (SplitsAssignment<SubscriptionSplit> sa : captor.getAllValues()) {
            for (List<SubscriptionSplit> splits : sa.assignment().values()) {
                splitCount += splits.size();
            }
        }
        assertThat(splitCount).isEqualTo(1);
    }

    @Test
    public void addSplitsBack_rereoutesSplits() {
        when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));
        when(mockContext.currentParallelism()).thenReturn(1);
        splitEnumerator.start();

        // Pretend reader 0 returned its split.
        SubscriptionSplit returned =
                SubscriptionSplit.create(ProjectSubscriptionName.of(PROJECT, "other-sub"));
        splitEnumerator.addSplitsBack(Collections.singletonList(returned), 0);

        // The returned split should be rerouted to a registered reader (reader 0, the only one).
        assertThat(assignedSubscriptionsFromCalls()).contains("other-sub");
    }

    @Test
    public void snapshotState_recordsAssignedSubscriptions() {
        when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));
        when(mockContext.currentParallelism()).thenReturn(1);
        splitEnumerator.start();

        PubSubEnumeratorCheckpoint checkpoint = splitEnumerator.snapshotState(0L);
        assertThat(checkpoint.getAssignedSubscriptionsList()).contains(SUBSCRIPTION);
        assertThat(checkpoint.getAssignmentsList()).isNotEmpty();
    }

    @Test
    public void restoreFromCheckpoint_skipsAlreadyAssignedSubscriptions() {
        when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));
        when(mockContext.currentParallelism()).thenReturn(1);
        splitEnumerator.start();
        PubSubEnumeratorCheckpoint checkpoint = splitEnumerator.snapshotState(0L);

        // Build a new enumerator restored from the checkpoint with the same initial subscriptions.
        SplitEnumeratorContext<SubscriptionSplit> ctx2 = mockContext;
        PubSubSplitEnumerator restored =
                new PubSubSplitEnumerator(
                        Collections.singleton(SUBSCRIPTION), PROJECT, ctx2, checkpoint);
        restored.start();

        // The restored enumerator should not duplicate the initial subscription.
        assertThat(restored.getAssignedSubscriptions()).containsExactly(SUBSCRIPTION);
    }
}
