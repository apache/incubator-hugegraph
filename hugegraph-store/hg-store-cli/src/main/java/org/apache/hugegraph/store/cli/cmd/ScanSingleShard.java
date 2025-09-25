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

package org.apache.hugegraph.store.cli.cmd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/18
 **/
@Slf4j
public class ScanSingleShard extends Command implements Scan {

    private volatile boolean closed = false;
    private AtomicInteger sum = new AtomicInteger();
    private ConcurrentHashMap<Long, StreamObserver<ScanPartitionRequest>>
            observers = new ConcurrentHashMap<>();

    public ScanSingleShard(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> getData(58, latch, "10.14.139.71:8500")).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("all rows are: {}", sum.get());
    }
}
