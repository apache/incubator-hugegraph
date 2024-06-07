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

package org.apache.hugegraph.store.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;

public class FutureClosure implements Closure, Future<Status> {

    private static final Logger LOG = LoggerFactory.getLogger(FutureClosure.class);

    private final CountDownLatch latch;
    private Status status;

    public FutureClosure() {
        this(1);
    }

    public FutureClosure(int count) {
        this.latch = new CountDownLatch(count);
    }

    public static void waitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("{}", e);
        }
    }

    @Override
    public void run(Status status) {
        this.status = status;
        latch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public Status get() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            status = new Status(RaftError.EINTR, e.getMessage());
        }
        return status;
    }

    @Override
    public Status get(long timeout, TimeUnit unit) {
        try {
            latch.await(timeout, unit);
        } catch (InterruptedException e) {
            status = new Status(RaftError.EINTR, e.getMessage());
        }
        return status;
    }
}
