/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.client.test.HgPDTestUtil;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.junit.BeforeClass;

public class PDPulseTest {
    private static PDClient pdClient;

    private final long storeId = 0;
    private final String storeAddress = "localhost";
    private final String graphName = "graph1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        PDConfig pdConfig = PDConfig.of("localhost:8686");
        pdConfig.setEnableCache(true);
        pdClient = PDClient.create(pdConfig);
        pdClient.getLeader();
    }

    // @Test
    public void listen() {

        PDPulse pulse = new PDPulseImpl(pdClient.getLeaderIp());
        CountDownLatch latch = new CountDownLatch(60);

        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier1 =
                pulse.connectPartition(new PulseListener(latch, "listener1"));
        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier2 =
                pulse.connectPartition(new PulseListener(latch, "listener2"));
        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier3 =
                pulse.connectPartition(new PulseListener(latch, "listener3"));

        try {
            latch.await(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PartitionHeartbeatRequest.Builder builder = PartitionHeartbeatRequest.newBuilder();

        notifier1.notifyServer(builder);


        notifier2.notifyServer(builder);

        notifier3.notifyServer(builder);

        notifier1.close();
        notifier2.close();
        notifier3.close();
    }


    private class PulseListener<T> implements PDPulse.Listener<T> {
        private final String listenerName;
        CountDownLatch latch = new CountDownLatch(10);

        private PulseListener(CountDownLatch latch, String listenerName) {
            this.latch = latch;
            this.listenerName = listenerName;
        }

        @Override
        public void onNext(T response) {
            // println(this.listenerName+" res: "+response);
            // this.latch.countDown();
        }

        @Override
        public void onNotice(PulseServerNotice<T> notice) {
            HgPDTestUtil.println(this.listenerName + " ---> res: " + notice.getContent());

            notice.ack();
            this.latch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            HgPDTestUtil.println(this.listenerName + " error: " + throwable.toString());
        }

        @Override
        public void onCompleted() {
            HgPDTestUtil.println(this.listenerName + " is completed");
        }
    }
}