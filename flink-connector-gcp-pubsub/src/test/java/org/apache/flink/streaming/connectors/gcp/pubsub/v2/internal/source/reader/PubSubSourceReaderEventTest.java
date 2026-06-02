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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubDeserializationSchemaV2;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event.RateLimitChangeEvent;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.split.SubscriptionSplit;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

/**
 * Tests for {@link PubSubSourceReader#handleSourceEvents(org.apache.flink.api.connector.source.SourceEvent)}
 * — covers rate-limit change handling added in Plan #1 Phase C and updated in the addendum.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceReaderEventTest {

    @Mock SplitReader<PubsubMessage, SubscriptionSplit> mockSplitReader;

    @Mock AckTracker mockAckTracker;

    @Mock(answer = RETURNS_DEEP_STUBS)
    SourceReaderContext mockContext;

    PubSubSourceReader<String> reader;

    @Before
    public void doBeforeEachTest() {
        reader =
                new PubSubSourceReader<>(
                        PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()),
                        mockAckTracker,
                        (PubSubSourceReader.LegacySplitReaderFactory)
                                (ackTracker) -> mockSplitReader,
                        new Configuration(),
                        mockContext);
    }

    @Test
    public void rateLimitChangeUpdatesEffectiveLimits() {
        Map<String, Long> overrides = new HashMap<>();
        overrides.put("sub-a", 500L);
        reader.handleSourceEvents(new RateLimitChangeEvent(200L, overrides));

        assertThat(reader.effectiveRateLimit("sub-a")).isEqualTo(500L);
        assertThat(reader.effectiveRateLimit("sub-b")).isEqualTo(200L);
    }

    @Test
    public void rateLimitOverrideClearedByNullValue() {
        Map<String, Long> overrides = new HashMap<>();
        overrides.put("sub-a", 500L);
        reader.handleSourceEvents(new RateLimitChangeEvent(100L, overrides));
        assertThat(reader.effectiveRateLimit("sub-a")).isEqualTo(500L);

        Map<String, Long> clearing = new HashMap<>();
        clearing.put("sub-a", null);
        reader.handleSourceEvents(new RateLimitChangeEvent(null, clearing));
        assertThat(reader.effectiveRateLimit("sub-a")).isEqualTo(100L);
    }

    @Test
    public void rateLimitChangeWithNullDefaultLeavesSourceLevelUntouched() {
        reader.handleSourceEvents(new RateLimitChangeEvent(50L, Collections.emptyMap()));
        assertThat(reader.effectiveRateLimit("anything")).isEqualTo(50L);

        Map<String, Long> overrides = new HashMap<>();
        overrides.put("sub-x", 999L);
        reader.handleSourceEvents(new RateLimitChangeEvent(null, overrides));
        assertThat(reader.effectiveRateLimit("anything")).isEqualTo(50L);
        assertThat(reader.effectiveRateLimit("sub-x")).isEqualTo(999L);
    }

    @Test
    public void effectiveRateLimitReturnsNullWhenNothingConfigured() {
        assertThat(reader.effectiveRateLimit("never-seen")).isNull();
    }
}
