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

import com.google.api.core.ApiFuture;
import com.google.pubsub.v1.PubsubMessage;

import java.util.Optional;

/** Pulls Pub/Sub messages and notifies the caller when more become available. */
public interface NotifyingPullSubscriber {
    /** Returns a {@link ApiFuture} that will be completed when messages are available to pull. */
    ApiFuture<Void> notifyDataAvailable();

    /** Pulls a message if one is available. */
    Optional<PubsubMessage> pullMessage() throws Throwable;

    /**
     * Interrupts an outstanding {@link ApiFuture} returned by {@link #notifyDataAvailable()}, if
     * any, so that the caller can stop waiting for new messages.
     */
    void interruptNotify();

    void shutdown();
}
