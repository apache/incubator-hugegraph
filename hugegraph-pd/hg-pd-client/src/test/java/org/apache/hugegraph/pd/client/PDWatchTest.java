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

import org.apache.hugegraph.pd.watch.NodeEvent;
import org.junit.BeforeClass;
// import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hugegraph.pd.client.test.HgPDTestUtil.println;

@Deprecated
public class PDWatchTest {

    private static PDClient pdClient;

    private long storeId = 0;
    private String storeAddr = "localhost";
    private String graphName = "graph1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        pdClient = PDClient.create(PDConfig.of("localhost:9000"));
    }

    // @Test
    public void watch() {
        PDWatch watch = pdClient.getWatchClient();
        CountDownLatch latch = new CountDownLatch(10);

        PDWatch.Watcher watcher1 = watch.watchPartition(new WatchListener(latch, "watcher1"));
        PDWatch.Watcher watcher2 = watch.watchPartition(new WatchListener(latch, "watcher2"));
        PDWatch.Watcher watcher3 = watch.watchPartition(new WatchListener(latch, "watcher3"));

        PDWatch.Watcher nodeWatcher1 =
                watch.watchNode(new WatchListener<NodeEvent>(latch, "nodeWatcher1"));

        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        watcher1.close();
        watcher2.close();
        watcher3.close();
    }

    private class WatchListener<T> implements PDWatch.Listener<T> {

        CountDownLatch latch = new CountDownLatch(10);
        private String watcherName;

        private WatchListener(CountDownLatch latch, String watcherName) {
            this.latch = latch;
            this.watcherName = watcherName;
        }

        @Override
        public void onNext(T response) {
            println(this.watcherName + " res: " + response);
            this.latch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            println(this.watcherName + " error: " + throwable.toString());
        }

        @Override
        public void onCompleted() {
            println(this.watcherName + " is completed");
        }
    }
}
