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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubDeserializationSchemaV2;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.RateLimitChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplitState;

import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Pub/Sub source reader.
 *
 * <p>Holds the live, mutable rate-limit configuration sourced from {@link RateLimitChangeEvent}s
 * broadcast by the enumerator. The {@link SplitReaderFactory} receives both the reader-owned
 * {@link AckTracker} and a {@code Function<splitId, Long>} that reads the latest effective
 * rate-limit from this reader on every call — that's what lets a live event take effect on the
 * next fetch without rebuilding the split reader (Plan #1 Phase C addendum, option (b)).
 */
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                PubsubMessage, T, SubscriptionSplit, SubscriptionSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceReader.class);

    /** Factory for the per-reader {@link SplitReader}. */
    public interface SplitReaderFactory {
        SplitReader<PubsubMessage, SubscriptionSplit> create(
                AckTracker ackTracker, Function<String, Long> rateLimitLookup);
    }

    /** Legacy factory preserved for tests that ignore the rate-limit lookup. */
    public interface LegacySplitReaderFactory {
        SplitReader<PubsubMessage, SubscriptionSplit> create(AckTracker ackTracker);
    }

    /**
     * Holder that breaks the constructor-bootstrap cycle: the rate-limit state lives here, so the
     * supplier passed to {@code super(...)} (before {@code this} is available) can still see a
     * stable reference and call into {@code lookup}, while the {@code PubSubSourceReader}
     * instance also exposes the same state to {@link #handleSourceEvents}.
     */
    private static final class RateLimitState {
        final AtomicReference<Long> sourceLevel = new AtomicReference<>();
        final ConcurrentHashMap<String, Long> perSplit = new ConcurrentHashMap<>();

        Long effective(String splitId) {
            Long override = perSplit.get(splitId);
            return override != null ? override : sourceLevel.get();
        }
    }

    private final AckTracker ackTracker;
    private final RateLimitState rateLimitState;

    public PubSubSourceReader(
            PubSubDeserializationSchemaV2<T> schema,
            AckTracker ackTracker,
            SplitReaderFactory splitReaderFactory,
            Configuration config,
            SourceReaderContext context) {
        this(schema, ackTracker, splitReaderFactory, new RateLimitState(), config, context);
    }

    /** Legacy constructor preserved for tests that pass an {@link LegacySplitReaderFactory}. */
    public PubSubSourceReader(
            PubSubDeserializationSchemaV2<T> schema,
            AckTracker ackTracker,
            LegacySplitReaderFactory legacyFactory,
            Configuration config,
            SourceReaderContext context) {
        this(
                schema,
                ackTracker,
                (at, ignoredLookup) -> legacyFactory.create(at),
                new RateLimitState(),
                config,
                context);
    }

    /**
     * Private constructor that lets the {@code super(...)} call capture the same {@code
     * RateLimitState} instance later stored in the field — sidestepping the JLS prohibition on
     * referencing {@code this} in a super-call argument.
     */
    private PubSubSourceReader(
            PubSubDeserializationSchemaV2<T> schema,
            AckTracker ackTracker,
            SplitReaderFactory splitReaderFactory,
            RateLimitState rateLimitState,
            Configuration config,
            SourceReaderContext context) {
        super(
                () -> splitReaderFactory.create(ackTracker, rateLimitState::effective),
                new PubSubRecordEmitter<>(schema, ackTracker),
                config,
                context);
        this.ackTracker = ackTracker;
        this.rateLimitState = rateLimitState;
    }

    @Override
    public List<SubscriptionSplit> snapshotState(long checkpointId) {
        ackTracker.addCheckpoint(checkpointId);
        return super.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        ackTracker.notifyCheckpointComplete(checkpointId);
    }

    @Override
    protected SubscriptionSplitState initializedState(SubscriptionSplit sourceSplit) {
        return new SubscriptionSplitState(sourceSplit);
    }

    @Override
    protected SubscriptionSplit toSplitType(String splitState, SubscriptionSplitState state) {
        return state.getSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, SubscriptionSplitState> map) {
        throw new IllegalStateException(
                "Splits should never become finished, since the source is unbounded.");
    }

    /**
     * Returns the effective rate limit for the given split id. Per-split override wins; falls
     * back to source-level default; falls back to {@code null} (no limit).
     */
    public Long effectiveRateLimit(String splitId) {
        return rateLimitState.effective(splitId);
    }

    @Override
    public void handleSourceEvents(SourceEvent event) {
        if (event instanceof RateLimitChangeEvent) {
            RateLimitChangeEvent e = (RateLimitChangeEvent) event;
            if (e.defaultLimit() != null) {
                rateLimitState.sourceLevel.set(e.defaultLimit());
            }
            for (Map.Entry<String, Long> entry : e.perSplitLimits().entrySet()) {
                if (entry.getValue() == null) {
                    rateLimitState.perSplit.remove(entry.getKey());
                } else {
                    rateLimitState.perSplit.put(entry.getKey(), entry.getValue());
                }
            }
            LOG.info(
                    "PubSubSourceReader: applied RateLimitChangeEvent (default={}, overrides={})",
                    e.defaultLimit(),
                    e.perSplitLimits());
            return;
        }
        super.handleSourceEvents(event);
    }
}
