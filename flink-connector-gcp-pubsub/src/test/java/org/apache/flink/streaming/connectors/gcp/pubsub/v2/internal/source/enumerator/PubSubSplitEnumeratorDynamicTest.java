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
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the dynamic-subscription API on {@link PubSubSplitEnumerator}.
 *
 * <p>After the Plan #1 Phase C addendum, every subscription is treated identically — including
 * subscriptions supplied at construction. The dedicated "skip source-default" behavior is gone, so
 * those-related tests have been retired.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitEnumeratorDynamicTest {

    private static final String PROJECT = "test-project";

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
        // Start with no initial subscriptions — exercise the dynamic addSubscriptions path.
        enumerator =
                new PubSubSplitEnumerator(
                        Collections.<String>emptySet(),
                        PROJECT,
                        mockContext,
                        /* checkpoint= */ null);
    }

    @Test
    public void addSubscriptionsAssignsToRegisteredReaders() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0, 1));
        when(mockContext.currentParallelism()).thenReturn(2);

        enumerator.addReader(0);
        enumerator.addReader(1);
        enumerator.addSubscriptions(new HashSet<>(java.util.Arrays.asList("sub-b", "sub-c")));

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
        // Sequenced stub: first call returns empty (parking happens), subsequent calls return
        // both readers so addReader can flush the parked split.
        when(mockContext.registeredReaders())
                .thenReturn(Collections.emptyMap())
                .thenReturn(registeredReaders(0, 1));
        when(mockContext.currentParallelism()).thenReturn(2);

        enumerator.addSubscriptions(Collections.singleton("sub-x"));
        verify(mockContext, never()).assignSplits(any());

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
        enumerator.addSubscriptions(Collections.singleton("sub-y"));
        int afterSecondAdd = enumerator.getAssignedSubscriptions().size();
        assertThat(afterSecondAdd).isEqualTo(afterFirstAdd);
        assertThat(enumerator.getAssignedSubscriptions()).contains("sub-y");
    }

    @Test
    public void addSubscriptionsTreatsAllSubscriptionsUniformly() {
        // Build an enumerator whose initial-subscription set carries "shared". After start(),
        // calling addSubscriptions("shared") again must be a no-op — proving the uniform-routing
        // model still de-duplicates against the initial set.
        PubSubSplitEnumerator e =
                new PubSubSplitEnumerator(
                        Collections.singleton("shared"),
                        PROJECT,
                        mockContext,
                        /* checkpoint= */ null);
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));
        when(mockContext.currentParallelism()).thenReturn(1);

        e.start();
        int afterStart = e.getAssignedSubscriptions().size();
        e.addSubscriptions(Collections.singleton("shared"));
        assertThat(e.getAssignedSubscriptions().size()).isEqualTo(afterStart);
        assertThat(e.getAssignedSubscriptions()).contains("shared");
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
    public void addSubscriptionsTracksAssignedForSnapshotState() {
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));
        when(mockContext.currentParallelism()).thenReturn(1);

        enumerator.addReader(0);
        enumerator.addSubscriptions(Collections.singleton("sub-b"));

        assertThat(enumerator.getAssignedSubscriptions()).containsExactly("sub-b");
    }

    @Test
    public void addSubscriptionsIgnoresProjectSubscriptionNameNamespacing() {
        // sanity-check the ProjectSubscriptionName.of(projectName, subName) round-trip.
        when(mockContext.registeredReaders()).thenReturn(registeredReaders(0));
        when(mockContext.currentParallelism()).thenReturn(1);

        enumerator.addReader(0);
        enumerator.addSubscriptions(Collections.singleton("ns-sub"));

        ArgumentCaptor<SplitsAssignment<SubscriptionSplit>> captor =
                ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(mockContext, org.mockito.Mockito.atLeastOnce()).assignSplits(captor.capture());
        boolean ok = false;
        for (SplitsAssignment<SubscriptionSplit> sa : captor.getAllValues()) {
            for (List<SubscriptionSplit> splits : sa.assignment().values()) {
                for (SubscriptionSplit s : splits) {
                    ProjectSubscriptionName psn = s.subscriptionName();
                    if (PROJECT.equals(psn.getProject()) && "ns-sub".equals(psn.getSubscription())) {
                        ok = true;
                    }
                }
            }
        }
        assertThat(ok).isTrue();
    }
}
