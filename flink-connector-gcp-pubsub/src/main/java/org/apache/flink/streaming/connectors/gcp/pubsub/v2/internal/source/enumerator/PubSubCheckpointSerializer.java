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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.gcp.pubsub.proto.PubSubEnumeratorCheckpoint;

import java.io.IOException;

/**
 * Versioned serializer for {@link PubSubEnumeratorCheckpoint}.
 *
 * <p>Version 1 (current): the proto now carries an {@code assigned_subscriptions} repeated field
 * (Plan #1 Phase C addendum). Older v0 checkpoints, which did not populate this field, deserialize
 * cleanly under v1 thanks to proto3's forward-compat semantics: missing repeated fields decode as
 * empty lists, and the enumerator constructor backfills {@code assignedSubscriptions} from each
 * restored split's subscription name. So no explicit migration branch is required.
 */
public class PubSubCheckpointSerializer
        implements SimpleVersionedSerializer<PubSubEnumeratorCheckpoint> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(PubSubEnumeratorCheckpoint message) {
        return message.toByteArray();
    }

    @Override
    public PubSubEnumeratorCheckpoint deserialize(int version, byte[] bytes) throws IOException {
        return PubSubEnumeratorCheckpoint.parseFrom(bytes);
    }
}
