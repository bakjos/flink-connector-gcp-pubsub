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

package org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PubSubSplitReader}. */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitReaderTest {

    @Mock Function<SubscriptionSplit, NotifyingPullSubscriber> mockFactory;

    @Mock NotifyingPullSubscriber mockSubscriber1;

    @Mock NotifyingPullSubscriber mockSubscriber2;

    PubSubSplitReader reader;

    @Before
    public void doBeforeEachTest() {
        reader = new PubSubSplitReader(mockFactory);
        when(mockFactory.apply(org.mockito.ArgumentMatchers.any(SubscriptionSplit.class)))
                .thenReturn(mockSubscriber1)
                .thenReturn(mockSubscriber2);
    }

    private static SubscriptionSplit createSplit() {
        return SubscriptionSplit.create(ProjectSubscriptionName.of("project", "sub"));
    }

    private static Map<String, List<PubsubMessage>> getSplitMessages(
            RecordsWithSplitIds<PubsubMessage> records) {
        Map<String, List<PubsubMessage>> out = new HashMap<>();
        for (String split = records.nextSplit(); split != null; split = records.nextSplit()) {
            for (PubsubMessage m = records.nextRecordFromSplit();
                    m != null;
                    m = records.nextRecordFromSplit()) {
                out.computeIfAbsent(split, k -> new ArrayList<>()).add(m);
            }
        }
        return out;
    }

    @Test
    public void noMessages_returnsEmpty() throws Throwable {
        RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(getSplitMessages(records)).isEmpty();
    }

    @Test
    public void subscriberCreateFails_propagatesError() throws Throwable {
        when(mockFactory.apply(org.mockito.ArgumentMatchers.any(SubscriptionSplit.class)))
                .thenThrow(new RuntimeException());

        assertThrows(
                RuntimeException.class,
                () ->
                        reader.handleSplitsChanges(
                                new SplitsAddition<>(Collections.singletonList(createSplit()))));
    }

    @Test
    public void fetchError_propagatesError() throws Throwable {
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        doAnswer(
                        invocation -> {
                            throw new RuntimeException();
                        })
                .when(mockSubscriber1)
                .pullMessage();
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(createSplit())));

        assertThrows(IOException.class, reader::fetch);
    }

    @Test
    public void fetch_oneSplitNoMessages() throws Throwable {
        SubscriptionSplit split1 = createSplit();
        SubscriptionSplit split2 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        PubsubMessage message =
                PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        when(mockSubscriber2.notifyDataAvailable()).thenReturn(SettableApiFuture.create());
        when(mockSubscriber1.pullMessage()).thenReturn(Optional.of(message));
        when(mockSubscriber2.pullMessage()).thenReturn(Optional.empty());

        RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
        Map<String, List<PubsubMessage>> expected = new HashMap<>();
        expected.put(split1.splitId(), Collections.singletonList(message));
        assertThat(getSplitMessages(records)).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    public void fetch_bothSplitsWithMessages() throws Throwable {
        SubscriptionSplit split1 = createSplit();
        SubscriptionSplit split2 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        PubsubMessage message1 =
                PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
        PubsubMessage message2 =
                PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        when(mockSubscriber2.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        when(mockSubscriber1.pullMessage()).thenReturn(Optional.of(message1));
        when(mockSubscriber2.pullMessage()).thenReturn(Optional.of(message2));

        RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
        Map<String, List<PubsubMessage>> expected = new HashMap<>();
        expected.put(split1.splitId(), Collections.singletonList(message1));
        expected.put(split2.splitId(), Collections.singletonList(message2));
        assertThat(getSplitMessages(records)).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    public void notifyFailure_propogatesError() throws Throwable {
        SubscriptionSplit split1 = createSplit();
        SubscriptionSplit split2 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        SettableApiFuture<Void> future1 = SettableApiFuture.create();
        SettableApiFuture<Void> future2 = SettableApiFuture.create();
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(future1);
        when(mockSubscriber2.notifyDataAvailable()).thenReturn(future2);

        Future<RecordsWithSplitIds<PubsubMessage>> fetchFuture =
                Executors.newSingleThreadExecutor()
                        .submit(
                                () -> {
                                    try {
                                        return reader.fetch();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
        assertThat(fetchFuture.isDone()).isFalse();

        future1.setException(new RuntimeException());
        assertThrows(ExecutionException.class, fetchFuture::get);
    }

    @Test
    public void interrupt_returnsEmptyMessages() throws Throwable {
        SubscriptionSplit split1 = createSplit();
        SubscriptionSplit split2 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        SettableApiFuture<Void> future1 = SettableApiFuture.create();
        SettableApiFuture<Void> future2 = SettableApiFuture.create();
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(future1);
        when(mockSubscriber2.notifyDataAvailable()).thenReturn(future2);
        when(mockSubscriber1.pullMessage()).thenReturn(Optional.empty());
        when(mockSubscriber2.pullMessage()).thenReturn(Optional.empty());
        doAnswer(
                        invocation -> {
                            future1.setException(
                                    new PubSubNotifyingPullSubscriber.SubscriberWakeupException());
                            return null;
                        })
                .when(mockSubscriber1)
                .interruptNotify();
        doAnswer(
                        invocation -> {
                            future2.setException(
                                    new PubSubNotifyingPullSubscriber.SubscriberWakeupException());
                            return null;
                        })
                .when(mockSubscriber2)
                .interruptNotify();

        Future<RecordsWithSplitIds<PubsubMessage>> fetchFuture =
                Executors.newSingleThreadExecutor()
                        .submit(
                                () -> {
                                    try {
                                        return reader.fetch();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
        assertThat(fetchFuture.isDone()).isFalse();

        reader.wakeUp();
        RecordsWithSplitIds<PubsubMessage> records = fetchFuture.get();
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(getSplitMessages(records)).isEmpty();
    }

    @Test
    public void closeFailure_allSubscribersShutdown() throws Throwable {
        SubscriptionSplit split1 = createSplit();
        SubscriptionSplit split2 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        doThrow(new RuntimeException()).when(mockSubscriber1).shutdown();

        assertThrows(RuntimeException.class, reader::close);
        verify(mockSubscriber1).shutdown();
        verify(mockSubscriber2).shutdown();
    }

    @Test
    public void secondSplitAdded_isIgnored() throws Exception {
        SubscriptionSplit split1 = createSplit();
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));
        verify(mockFactory, times(1))
                .apply(org.mockito.ArgumentMatchers.any(SubscriptionSplit.class));
    }

    @Test
    public void unknownSplitChange_throwsError() {
        assertThrows(IllegalArgumentException.class, () -> reader.handleSplitsChanges(null));
    }

    @Test
    public void fetch_appliesRateLimitFromLookup() throws Throwable {
        // Wire a rate-limit lookup that returns 1 msg/sec for split1, null for others. With the
        // first fetch starting at t=0, the second fetch should request a ~1s sleep before
        // proceeding. Use a fake sleep + time source to assert without actually waiting.
        long[] currentNanos = {0L};
        long[] requestedSleep = {0L};
        PubSubSplitReader r =
                new PubSubSplitReader(
                        mockFactory,
                        (splitId) -> 1L,
                        nanos -> {
                            requestedSleep[0] = nanos;
                            currentNanos[0] += nanos;
                        },
                        () -> currentNanos[0]);

        SubscriptionSplit split1 = createSplit();
        when(mockFactory.apply(org.mockito.ArgumentMatchers.any(SubscriptionSplit.class)))
                .thenReturn(mockSubscriber1);
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        when(mockSubscriber1.pullMessage()).thenReturn(Optional.empty());
        r.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));

        // First fetch records lastFetchNanos; no sleep yet (no prior fetch to throttle against).
        r.fetch();
        assertThat(requestedSleep[0]).isEqualTo(0L);

        // Immediately fetch again — the throttle should ask for ~1e9 nanos sleep.
        long beforeSecondFetch = currentNanos[0];
        r.fetch();
        // The throttle sleeps just enough so the elapsed-since-last-fetch reaches 1 second.
        // Allow a 1ms slack so we don't depend on integer-arithmetic exactness.
        assertThat(requestedSleep[0]).isGreaterThan(990_000_000L);
        assertThat(currentNanos[0]).isGreaterThanOrEqualTo(beforeSecondFetch);
    }

    @Test
    public void fetch_skipsThrottleWhenRateLimitNull() throws Throwable {
        long[] currentNanos = {0L};
        long[] requestedSleep = {0L};
        PubSubSplitReader r =
                new PubSubSplitReader(
                        mockFactory,
                        (splitId) -> null,
                        nanos -> {
                            requestedSleep[0] = nanos;
                            currentNanos[0] += nanos;
                        },
                        () -> currentNanos[0]);

        SubscriptionSplit split1 = createSplit();
        when(mockFactory.apply(org.mockito.ArgumentMatchers.any(SubscriptionSplit.class)))
                .thenReturn(mockSubscriber1);
        when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
        when(mockSubscriber1.pullMessage()).thenReturn(Optional.empty());
        r.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));

        r.fetch();
        r.fetch();
        assertThat(requestedSleep[0]).isEqualTo(0L);
    }
}
