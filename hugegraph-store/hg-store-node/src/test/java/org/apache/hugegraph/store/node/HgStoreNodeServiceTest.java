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

package org.apache.hugegraph.store.node;

// import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HgStore单元测试
 * 1、测试raft多副本入库
 * 2、测试快照同步
 * 3、测试副本增减
 * 4、测试单幅本关闭日志入库
 */
public class HgStoreNodeServiceTest {

    String yml =
            "rocksdb:\n" + "  # rocksdb 使用的总内存大小\n" + "  total_memory_size: 32000000000\n" +
            "  max_background_jobs: 8\n" + "  max_subcompactions: 4\n" +
            "  target_file_size_multiplier: 4\n" + "  min_write_buffer_number_to_merge: 8\n" +
            "  target_file_size_base: 512000000";

    // @Test
    public void testRaft() {

    }

    // @Test
    public void testYaml() throws InterruptedException, IOException {

        ExecutorService executor = new ThreadPoolExecutor(1000, 1000,
                                                          10L, TimeUnit.SECONDS,
                                                          new ArrayBlockingQueue<>(10000));
        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {

            executor.execute(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(executor);
                System.out.println(Thread.activeCount());
                latch.countDown();
            });

        }
        latch.await();
        System.in.read();
    }
}
