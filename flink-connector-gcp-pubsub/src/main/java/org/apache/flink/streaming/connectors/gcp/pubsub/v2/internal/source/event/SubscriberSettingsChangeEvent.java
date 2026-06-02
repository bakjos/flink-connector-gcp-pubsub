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

import java.util.Objects;

/**
 * Emitted by the {@code PubSubSplitEnumerator} to readers when subscriber-level gRPC settings
 * change. {@code null} fields mean "unchanged". Readers respond by rebuilding their per-split
 * subscribers with the new settings.
 */
public final class SubscriberSettingsChangeEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Integer requestTimeoutSec;
    private final Integer requestRetries;

    public SubscriberSettingsChangeEvent(Integer requestTimeoutSec, Integer requestRetries) {
        this.requestTimeoutSec = requestTimeoutSec;
        this.requestRetries = requestRetries;
    }

    public Integer requestTimeoutSec() {
        return requestTimeoutSec;
    }

    public Integer requestRetries() {
        return requestRetries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubscriberSettingsChangeEvent)) {
            return false;
        }
        SubscriberSettingsChangeEvent e = (SubscriberSettingsChangeEvent) o;
        return Objects.equals(requestTimeoutSec, e.requestTimeoutSec)
                && Objects.equals(requestRetries, e.requestRetries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestTimeoutSec, requestRetries);
    }
}
