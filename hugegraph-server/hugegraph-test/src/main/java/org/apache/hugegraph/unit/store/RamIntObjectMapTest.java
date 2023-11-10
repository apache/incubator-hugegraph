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

package org.apache.hugegraph.unit.store;

import java.util.concurrent.CountDownLatch;

import org.apache.hugegraph.backend.store.ram.IntObjectMap;
import org.junit.Assert;
import org.junit.Test;

public class RamIntObjectMapTest {

    @Test
    public void testConcurrency() {
        int size = 32;
        IntObjectMap<Integer> map = new IntObjectMap<>(size);

        final int numThreads = 10;
        final CountDownLatch startSignal = new CountDownLatch(1);
        final CountDownLatch doneSignal = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }

                for (int j = 0; j < size; j++) {
                    map.set(j, j);
                }

                doneSignal.countDown();
            }).start();
        }

        startSignal.countDown();

        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                for (int j = 0; j < size; j++) {
                    Integer value = map.get(j);
                    Assert.assertNotNull(value);
                }
            }).start();
        }
    }
}
