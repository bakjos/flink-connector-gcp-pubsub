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
package org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Emitted by the {@code PubSubSplitEnumerator} to readers when the rate-limit configuration
 * changes. {@code defaultLimit} is the new source-level default ({@code null} = unchanged). The
 * {@code perSplitLimits} map carries per-split overrides — a {@code null} value clears the
 * override.
 */
public final class RateLimitChangeEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Long defaultLimit;
    private final Map<String, Long> perSplitLimits;

    public RateLimitChangeEvent(Long defaultLimit, Map<String, Long> perSplitLimits) {
        this.defaultLimit = defaultLimit;
        // Map.copyOf rejects null values (we use null to mean "clear override"), so use a
        // LinkedHashMap defensive copy that permits nulls.
        LinkedHashMap<String, Long> copy = new LinkedHashMap<>();
        if (perSplitLimits != null) {
            copy.putAll(perSplitLimits);
        }
        this.perSplitLimits = Collections.unmodifiableMap(copy);
    }

    public Long defaultLimit() {
        return defaultLimit;
    }

    public Map<String, Long> perSplitLimits() {
        return perSplitLimits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RateLimitChangeEvent)) {
            return false;
        }
        RateLimitChangeEvent e = (RateLimitChangeEvent) o;
        return Objects.equals(defaultLimit, e.defaultLimit)
                && Objects.equals(perSplitLimits, e.perSplitLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultLimit, perSplitLimits);
    }
}
