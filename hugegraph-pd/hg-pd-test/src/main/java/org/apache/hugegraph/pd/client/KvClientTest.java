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

    private KvClient<WatchResponse> client;

    @Before
    public void setUp() {
        client = new KvClient<>(getPdConfig());
    }

    @Test
    public void testCreateStub() {
        // Setup
        // Run the test
        try {
            final AbstractStub result = client.createStub();
            assertThat(result).isNotNull();
        } catch (Exception e) {
            org.junit.Assert.fail("createStub exception: " + e);
        } finally {
        }
    }

    @Test
    public void testCreateBlockingStub() {
        // Setup
        // Run the test
        try {
            final AbstractBlockingStub result = client.createBlockingStub();
            assertThat(result).isNotNull();
        } catch (Exception e) {
            org.junit.Assert.fail("createBlockingStub exception: " + e);
        } finally {
        }
    }

    String key = "key";
    String value = "value";

    @Test
    public void testPutAndGet() throws Exception {
        // Run the test
        try {
            client.put(key, value);
            // Run the test
            KResponse result = client.get(key);

            // Verify the results
            assertThat(result.getValue()).isEqualTo(value);
            client.delete(key);
            result = client.get(key);
            assertThat(StringUtils.isEmpty(result.getValue()));
            client.deletePrefix(key);
            client.put(key + "1", value);
            client.put(key + "2", value);
            ScanPrefixResponse response = client.scanPrefix(key);
            assertThat(response.getKvsMap().size() == 2);
            client.putTTL(key + "3", value, 1000);
            client.keepTTLAlive(key + "3");
            final Consumer<WatchResponse> mockConsumer = mock(Consumer.class);

            // Run the test
            client.listen(key + "3", mockConsumer);
            client.listenPrefix(key + "4", mockConsumer);
            WatchResponse r = WatchResponse.newBuilder().addEvents(
                                                   WatchEvent.newBuilder().setCurrent(
                                                           WatchKv.newBuilder().setKey(key).setValue("value")
                                                                  .build()).setType(WatchType.Put).build())
                                           .setClientId(0L)
                                           .setState(WatchState.Starting)
                                           .build();
            client.getWatchList(r);
            client.getWatchMap(r);
            client.lock(key, 3000L);
            client.isLocked(key);
            client.unlock(key);
            client.lock(key, 3000L);
            client.keepAlive(key);
            client.close();
        } catch (Exception e) {

        }
    }
}
