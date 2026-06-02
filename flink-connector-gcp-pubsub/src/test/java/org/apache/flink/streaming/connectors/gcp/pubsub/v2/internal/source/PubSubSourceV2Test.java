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

package org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubDeserializationSchemaV2;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link PubSubSource}. */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceV2Test {
    @Test
    public void build_invalidSubscription() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSource.<String>builder().setSubscriptionName(null));
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()));
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_invalidProject() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSource.<String>builder().setProjectName(null));
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder()
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()));
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_invalidSchema() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSource.<String>builder().setDeserializationSchema(null));
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder().setProjectName("project").setSubscriptionName("sub");
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_nullMaxOutstandingMessagesCountAllowed() throws Exception {
        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setMaxOutstandingMessagesCount(null)
                        .build();
        assertNull(source.maxOutstandingMessagesCount());
    }

    @Test
    public void build_negativeMaxOutstandingMessagesCountThrows() throws Exception {
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setMaxOutstandingMessagesCount(-1L);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_nullMaxOutstandingMessagesBytesAllowed() throws Exception {
        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setMaxOutstandingMessagesBytes(null)
                        .build();
        assertNull(source.maxOutstandingMessagesBytes());
    }

    @Test
    public void build_negativeMaxOutstandingMessagesBytesThrows() throws Exception {
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setMaxOutstandingMessagesBytes(-1L);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_nullParallelPullCountAllowed() throws Exception {
        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setParallelPullCount(null)
                        .build();
        assertNull(source.parallelPullCount());
    }

    @Test
    public void build_negativeParallelPullCountThrows() throws Exception {
        PubSubSource.Builder<String> builder =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setParallelPullCount(-1);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_nullCredentialsAllowed() throws Exception {
        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setCredentials(null)
                        .build();
        assertNull(source.credentials());
    }

    @Test
    public void build_nullEndpointAllowed() throws Exception {
        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setProjectName("project")
                        .setSubscriptionName("subscription")
                        .setDeserializationSchema(
                                PubSubDeserializationSchemaV2.dataOnly(new SimpleStringSchema()))
                        .setEndpoint(null)
                        .build();
        assertNull(source.endpoint());
    }
}
