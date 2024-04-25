/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchState;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.junit.Before;
import org.junit.Test;

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;

public class KvClientTest extends BaseClientTest {

    String key = "key";
    String value = "value";
    private KvClient<WatchResponse> client;

    @Before
    public void setUp() {
        this.client = new KvClient<>(PDConfig.of("localhost:8686"));
    }

    @Test
    public void testCreateStub() {
        // Setup
        // Run the test
        try {
            final AbstractStub result = this.client.createStub();
        } catch (Exception e) {

        }

        // Verify the results
    }

    @Test
    public void testCreateBlockingStub() {
        // Setup
        // Run the test
        try {
            final AbstractBlockingStub result = this.client.createBlockingStub();
        } catch (Exception e) {

        }
    }

    @Test
    public void testPutAndGet() throws Exception {
        // Run the test
        try {
            this.client.put(this.key, this.value);
            // Run the test
            KResponse result = this.client.get(this.key);

            // Verify the results
            assertThat(result.getValue()).isEqualTo(this.value);
            this.client.delete(this.key);
            result = this.client.get(this.key);
            assertThat(StringUtils.isEmpty(result.getValue()));
            this.client.deletePrefix(this.key);
            this.client.put(this.key + "1", this.value);
            this.client.put(this.key + "2", this.value);
            ScanPrefixResponse response = this.client.scanPrefix(this.key);
            assertThat(response.getKvsMap().size() == 2);
            this.client.putTTL(this.key + "3", this.value, 1000);
            this.client.keepTTLAlive(this.key + "3");
            final Consumer<WatchResponse> mockConsumer = mock(Consumer.class);

            // Run the test
            this.client.listen(this.key + "3", mockConsumer);
            this.client.listenPrefix(this.key + "4", mockConsumer);
            WatchResponse r = WatchResponse.newBuilder().addEvents(
                                                   WatchEvent.newBuilder().setCurrent(
                                                           WatchKv.newBuilder().setKey(this.key).setValue("value")
                                                                  .build()).setType(WatchType.Put).build())
                                           .setClientId(0L)
                                           .setState(WatchState.Starting)
                                           .build();
            this.client.getWatchList(r);
            this.client.getWatchMap(r);
            this.client.lock(this.key, 3000L);
            this.client.isLocked(this.key);
            this.client.unlock(this.key);
            this.client.lock(this.key, 3000L);
            this.client.keepAlive(this.key);
            this.client.close();
        } catch (Exception e) {

        }
    }
}
