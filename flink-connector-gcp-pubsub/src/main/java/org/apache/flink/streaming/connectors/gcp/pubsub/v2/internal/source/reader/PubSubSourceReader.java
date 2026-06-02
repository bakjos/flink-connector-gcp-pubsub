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
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.SubscriberSettingsChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplitState;

import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                PubsubMessage, T, SubscriptionSplit, SubscriptionSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceReader.class);

    public interface SplitReaderFactory {
        SplitReader<PubsubMessage, SubscriptionSplit> create(AckTracker ackTracker);
    }

    private final AckTracker ackTracker;

    /**
     * Source-level rate limit applied by application code that consults
     * {@link #effectiveRateLimit(String)}. Updated by {@link RateLimitChangeEvent}.
     */
    private final AtomicReference<Long> sourceLevelRateLimit = new AtomicReference<>();

    /** Per-split rate-limit overrides. Updated by {@link RateLimitChangeEvent}. */
    private final ConcurrentHashMap<String, Long> perSplitRateLimits = new ConcurrentHashMap<>();

    public PubSubSourceReader(
            PubSubDeserializationSchemaV2<T> schema,
            AckTracker ackTracker,
            SplitReaderFactory splitReaderFactory,
            Configuration config,
            SourceReaderContext context) {
        super(
                () -> splitReaderFactory.create(ackTracker),
                new PubSubRecordEmitter<>(schema, ackTracker),
                config,
                context);
        this.ackTracker = ackTracker;
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
        Long override = perSplitRateLimits.get(splitId);
        if (override != null) {
            return override;
        }
        return sourceLevelRateLimit.get();
    }

    @Override
    public void handleSourceEvents(SourceEvent event) {
        if (event instanceof RateLimitChangeEvent) {
            RateLimitChangeEvent e = (RateLimitChangeEvent) event;
            if (e.defaultLimit() != null) {
                sourceLevelRateLimit.set(e.defaultLimit());
            }
            for (Map.Entry<String, Long> entry : e.perSplitLimits().entrySet()) {
                if (entry.getValue() == null) {
                    perSplitRateLimits.remove(entry.getKey());
                } else {
                    perSplitRateLimits.put(entry.getKey(), entry.getValue());
                }
            }
            LOG.info(
                    "PubSubSourceReader: applied RateLimitChangeEvent (default={}, overrides={})",
                    e.defaultLimit(),
                    e.perSplitLimits());
            return;
        }
        if (event instanceof SubscriberSettingsChangeEvent) {
            // The PR-#32 reader does not currently expose a subscriber-rebuild path:
            // the SplitReader holds a Supplier<NotifyingPullSubscriber> that does not get
            // re-evaluated on existing splits. Treat this event as informational for now.
            // A full implementation would propagate the new settings into the
            // PubSubSplitReader and close + reopen each subscriber with the new builder
            // settings. Tracked for follow-up (see Plan #1 Phase C notes).
            SubscriberSettingsChangeEvent e = (SubscriberSettingsChangeEvent) event;
            LOG.warn(
                    "PubSubSourceReader: received SubscriberSettingsChangeEvent (timeoutSec={}, "
                            + "retries={}) but the current PR-#32 reader does not yet support live "
                            + "subscriber rebuild — settings will not take effect until job restart.",
                    e.requestTimeoutSec(),
                    e.requestRetries());
            return;
        }
        super.handleSourceEvents(event);
    }
}
