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

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@link SplitReader} that pulls {@link PubsubMessage}s from one or more Pub/Sub subscriptions.
 *
 * <p>Each {@link SubscriptionSplit} added via {@link #handleSplitsChanges(SplitsChange)} gets its
 * own {@link NotifyingPullSubscriber}, constructed lazily from the per-split subscriber factory.
 * This is what lets the uniformly-multi-subscription enumerator drive this reader: one split per
 * subscription, one subscriber per split.
 *
 * <p>Rate-limit wiring (Plan #1 Phase C addendum): an optional per-split rate-limit supplier is
 * consulted at the outer fetch boundary. We chose this approach (option (b) from the addendum
 * brief) because PR #32's {@link NotifyingPullSubscriber} interface takes no rate-limit input —
 * its flow control is configured at subscriber construction. Outer-boundary throttling lets the
 * limit react to live {@code RateLimitChangeEvent}s without rebuilding subscribers. The limit is
 * interpreted as messages-per-second; before each successful fetch we sleep just long enough to
 * stay under the configured cap, per split, computed from the previous-fetch timestamp.
 */
public class PubSubSplitReader implements SplitReader<PubsubMessage, SubscriptionSplit> {
    private final Map<String, NotifyingPullSubscriber> subscribers;
    private final Map<String, SubscriptionSplit> splitsById;
    private final Function<SubscriptionSplit, NotifyingPullSubscriber> factory;

    /**
     * Supplier of the effective rate limit (messages-per-second) for a given split id. Returns
     * {@code null} (or an Optional.empty equivalent) to mean "no limit". Wired by {@link
     * PubSubSourceReader} so that live {@code RateLimitChangeEvent}s take effect on the next
     * fetch.
     */
    private final Function<String, Long> rateLimitLookup;

    /** Per-split timestamp of the last successful fetch return (nanos). Used to compute sleep. */
    private final Map<String, AtomicLong> lastFetchNanos = new HashMap<>();

    /** Sleep primitive — overridable for tests. */
    private final SleepFn sleepFn;

    /** Time source — overridable for tests. */
    private final LongSupplier nanoTime;

    public PubSubSplitReader(Function<SubscriptionSplit, NotifyingPullSubscriber> factory) {
        this(factory, (splitId) -> null);
    }

    public PubSubSplitReader(
            Function<SubscriptionSplit, NotifyingPullSubscriber> factory,
            Function<String, Long> rateLimitLookup) {
        this(factory, rateLimitLookup, TimeUnit.NANOSECONDS::sleep, System::nanoTime);
    }

    PubSubSplitReader(
            Function<SubscriptionSplit, NotifyingPullSubscriber> factory,
            Function<String, Long> rateLimitLookup,
            SleepFn sleepFn,
            LongSupplier nanoTime) {
        this.factory = factory;
        this.rateLimitLookup = rateLimitLookup;
        this.subscribers = new HashMap<>();
        this.splitsById = new HashMap<>();
        this.sleepFn = sleepFn;
        this.nanoTime = nanoTime;
    }

    private Multimap<String, PubsubMessage> getMessages() throws Throwable {
        ImmutableListMultimap.Builder<String, PubsubMessage> messages =
                ImmutableListMultimap.builder();
        for (Map.Entry<String, NotifyingPullSubscriber> entry : subscribers.entrySet()) {
            for (PubsubMessage m : entry.getValue().pullMessage().asSet()) {
                messages.put(entry.getKey(), m);
            }
        }
        return messages.build();
    }

    private ApiFuture<Void> notifyDataAvailable() {
        SettableApiFuture<Void> future = SettableApiFuture.create();
        subscribers
                .values()
                .forEach(
                        s -> {
                            ApiFuture<Void> notification = s.notifyDataAvailable();
                            ApiFutures.addCallback(
                                    notification,
                                    new ApiFutureCallback<Void>() {
                                        @Override
                                        public void onFailure(Throwable t) {
                                            // Ignore exceptions caused by wakeups
                                            if (t
                                                    instanceof
                                                    PubSubNotifyingPullSubscriber
                                                            .SubscriberWakeupException) {
                                                future.set(null);
                                            }
                                            future.setException(t);
                                        }

                                        @Override
                                        public void onSuccess(Void result) {
                                            future.set(null);
                                        }
                                    },
                                    MoreExecutors.directExecutor());
                        });
        return future;
    }

    /**
     * Applies the per-split rate limit (option (b) in the addendum: throttle at the reader's outer
     * fetch boundary). For each tracked split with a non-null effective limit, sleeps just long
     * enough since the last fetch return to stay under {@code limit} messages-per-second. We use
     * the minimum required sleep across splits so we don't starve unthrottled splits.
     */
    private void applyRateLimitThrottle() throws IOException {
        long maxSleepNanos = 0L;
        long now = nanoTime.getAsLong();
        for (String splitId : subscribers.keySet()) {
            Long limit = rateLimitLookup.apply(splitId);
            if (limit == null || limit <= 0L) {
                continue;
            }
            AtomicLong last = lastFetchNanos.get(splitId);
            if (last == null) {
                continue;
            }
            long minIntervalNanos = TimeUnit.SECONDS.toNanos(1L) / limit;
            long elapsed = now - last.get();
            long remaining = minIntervalNanos - elapsed;
            if (remaining > maxSleepNanos) {
                maxSleepNanos = remaining;
            }
        }
        if (maxSleepNanos > 0L) {
            try {
                sleepFn.sleepNanos(maxSleepNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while applying rate limit throttle", e);
            }
        }
    }

    @Override
    public RecordsBySplits<PubsubMessage> fetch() throws IOException {
        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        if (subscribers.isEmpty()) {
            return builder.build();
        }
        applyRateLimitThrottle();
        try {
            notifyDataAvailable().get();
            getMessages().asMap().forEach(builder::addAll);
        } catch (Throwable t) {
            throw new IOException(t);
        }
        long now = nanoTime.getAsLong();
        for (String splitId : subscribers.keySet()) {
            lastFetchNanos
                    .computeIfAbsent(splitId, k -> new AtomicLong())
                    .set(now);
        }
        return builder.build();
    }

    @Override
    public synchronized void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new IllegalArgumentException("Unexpected split event " + splitsChange);
        }
        for (SubscriptionSplit newSplit : splitsChange.splits()) {
            subscribers.computeIfAbsent(newSplit.splitId(), (splitId) -> factory.apply(newSplit));
            splitsById.putIfAbsent(newSplit.splitId(), newSplit);
        }
    }

    @Override
    public void wakeUp() {
        subscribers.forEach((split, subscriber) -> subscriber.interruptNotify());
    }

    @Override
    public void close() throws Exception {
        Exception exception = null;
        for (NotifyingPullSubscriber subscriber : subscribers.values()) {
            try {
                subscriber.shutdown();
            } catch (Exception e) {
                exception = e;
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /** Sleep primitive — overridable in tests so we don't actually wait. */
    interface SleepFn {
        void sleepNanos(long nanos) throws InterruptedException;
    }
}
