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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.RateLimitChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.SubscriberSettingsChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the dynamic-subscription extension methods (addSubscriptions, updateRateLimits,
 * updateSubscriberSettings) added in Plan #1 Phase C.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitEnumeratorDynamicTest {

    private static final String PROJECT = "test-project";
    private static final ProjectSubscriptionName SOURCE_DEFAULT_SUB =
            ProjectSubscriptionName.of(PROJECT, "source-default-sub");

    @Mock SplitEnumeratorContext<SubscriptionSplit> mockContext;

    PubSubSplitEnumerator enumerator;

    private Map<Integer, ReaderInfo> registeredReaders(Integer... readers) {
        Map<Integer, ReaderInfo> out = new HashMap<>();
        for (Integer r : readers) {
            out.put(r, new ReaderInfo(r, "host-" + r));
        }
        return out;
    }

    @Before
    public void doBeforeEachTest() {
        enumerator =
                new PubSubSplitEnumerator(
                        SOURCE_DEFAULT_SUB, mockContext, new HashMap<Integer, SubscriptionSplit>());
    }

    @Test
    public void addSubscriptionsAssignsToRegisteredReaders() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0, 1));
        when(mockContext.currentParallelism()).thenReturn(2);

        enumerator.addReader(0);
        enumerator.addReader(1);

        // Now add dynamic subscriptions.
        enumerator.addSubscriptions(new HashSet<>(java.util.Arrays.asList("sub-b", "sub-c")));

        // Capture every SplitsAssignment call and verify sub-b + sub-c appear.
        ArgumentCaptor<SplitsAssignment<SubscriptionSplit>> captor =
                ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(mockContext, org.mockito.Mockito.atLeastOnce()).assignSplits(captor.capture());

        Set<String> assignedSubscriptions = new HashSet<>();
        for (SplitsAssignment<SubscriptionSplit> assignment : captor.getAllValues()) {
            for (Map.Entry<Integer, List<SubscriptionSplit>> e :
                    assignment.assignment().entrySet()) {
                for (SubscriptionSplit s : e.getValue()) {
                    assignedSubscriptions.add(s.subscriptionName().getSubscription());
                }
            }
        }
        assertThat(assignedSubscriptions).contains("sub-b", "sub-c");
    }

    @Test
    public void addSubscriptionsParksWhenReaderUnregistered() {
        when(mockContext.registeredReaders()).thenReturn(Collections.emptyMap());
        when(mockContext.currentParallelism()).thenReturn(2);

        // No readers registered.
        enumerator.addSubscriptions(Collections.singleton("sub-x"));

        // No assignment should have happened.
        verify(mockContext, never()).assignSplits(any());

        // Compute the reader the hash routes sub-x to and register that reader.
        SubscriptionSplit probeSplit =
                SubscriptionSplit.create(ProjectSubscriptionName.of(PROJECT, "sub-x"));
        // The enumerator created a different split with its own random uid, so we cannot
        // recompute the exact target subtask externally. Instead, register ALL possible
        // subtasks and assert that at least one assignSplits call delivers sub-x.
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0, 1));

        enumerator.addReader(0);
        enumerator.addReader(1);

        ArgumentCaptor<SplitsAssignment<SubscriptionSplit>> captor =
                ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(mockContext, org.mockito.Mockito.atLeastOnce()).assignSplits(captor.capture());

        boolean sawSubX = false;
        for (SplitsAssignment<SubscriptionSplit> assignment : captor.getAllValues()) {
            for (List<SubscriptionSplit> splits : assignment.assignment().values()) {
                for (SubscriptionSplit s : splits) {
                    if ("sub-x".equals(s.subscriptionName().getSubscription())) {
                        sawSubX = true;
                    }
                }
            }
        }
        assertThat(sawSubX).isTrue();
    }

    @Test
    public void addSubscriptionsIsIdempotent() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));
        when(mockContext.currentParallelism()).thenReturn(1);

        enumerator.addReader(0);
        enumerator.addSubscriptions(Collections.singleton("sub-y"));
        int afterFirstAdd = enumerator.getAssignedSubscriptions().size();
        enumerator.addSubscriptions(Collections.singleton("sub-y")); // duplicate
        int afterSecondAdd = enumerator.getAssignedSubscriptions().size();
        assertThat(afterSecondAdd).isEqualTo(afterFirstAdd);
        assertThat(enumerator.getAssignedSubscriptions()).contains("sub-y");
    }

    @Test
    public void addSubscriptionsSkipsSourceDefault() {
        org.mockito.Mockito.lenient()
                .when(mockContext.registeredReaders())
                .thenReturn(registeredReaders(0));
        org.mockito.Mockito.lenient().when(mockContext.currentParallelism()).thenReturn(1);

        enumerator.addSubscriptions(Collections.singleton(SOURCE_DEFAULT_SUB.getSubscription()));

        assertThat(enumerator.getAssignedSubscriptions())
                .doesNotContain(SOURCE_DEFAULT_SUB.getSubscription());
    }

    @Test
    public void updateRateLimitsBroadcastsEventToAllRegisteredReaders() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0, 1));

        Map<String, Long> overrides = new HashMap<>();
        overrides.put("sub-a", 500L);
        enumerator.updateRateLimits(200L, overrides);

        ArgumentCaptor<SourceEvent> evCaptor = ArgumentCaptor.forClass(SourceEvent.class);
        verify(mockContext, times(2)).sendEventToSourceReader(anyInt(), evCaptor.capture());

        for (SourceEvent ev : evCaptor.getAllValues()) {
            assertThat(ev).isInstanceOf(RateLimitChangeEvent.class);
            RateLimitChangeEvent rl = (RateLimitChangeEvent) ev;
            assertThat(rl.defaultLimit()).isEqualTo(200L);
            assertThat(rl.perSplitLimits()).containsEntry("sub-a", 500L);
        }
    }

    @Test
    public void updateSubscriberSettingsBroadcastsEvent() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));

        enumerator.updateSubscriberSettings(30, 5);

        ArgumentCaptor<SourceEvent> evCaptor = ArgumentCaptor.forClass(SourceEvent.class);
        verify(mockContext, times(1)).sendEventToSourceReader(anyInt(), evCaptor.capture());
        SourceEvent ev = evCaptor.getValue();
        assertThat(ev).isInstanceOf(SubscriberSettingsChangeEvent.class);
        SubscriberSettingsChangeEvent ss = (SubscriberSettingsChangeEvent) ev;
        assertThat(ss.requestTimeoutSec()).isEqualTo(30);
        assertThat(ss.requestRetries()).isEqualTo(5);
    }

    @Test
    public void addSubscriptionsTracksAssignedForSnapshotState() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));
        when(mockContext.currentParallelism()).thenReturn(1);

        enumerator.addReader(0);
        enumerator.addSubscriptions(Collections.singleton("sub-b"));

        // The enumerator's internal "assignedSubscriptions" tracks dynamic additions for
        // observability — snapshotState itself serializes per-reader splits including the
        // dynamically-added ones (which we already verified are assigned in
        // addSubscriptionsAssignsToRegisteredReaders).
        assertThat(enumerator.getAssignedSubscriptions()).containsExactly("sub-b");
    }
}
