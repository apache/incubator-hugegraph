/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.meta.lock;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.Log;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;

public class DistributedLock {

    protected static final Logger LOG = Log.logger(DistributedLock.class);

    private static DistributedLock lockProvider = null;
    private final static Object mutex = new Object();

    private Lock lockClient;
    private Lease leaseClient;

    private DistributedLock(Client client) {
        this.lockClient = client.getLockClient();
        this.leaseClient = client.getLeaseClient();
    }

    public static DistributedLock getInstance(Client client) {
        synchronized (mutex) {
            if (null == lockProvider) {
                lockProvider = new DistributedLock(client);
            }
        }
        return lockProvider;
    }

    public LockResult lock(String lockName, long ttl) {
        LockResult lockResult = new LockResult();
        ScheduledExecutorService service =
                                 Executors.newSingleThreadScheduledExecutor();

        lockResult.lockSuccess(false);
        lockResult.setService(service);

        Long leaseId;

        try {
            leaseId = this.leaseClient.grant(ttl).get().getID();
            lockResult.setLeaseId(leaseId);

            long period = ttl - ttl / 5;
            service.scheduleAtFixedRate(new KeepAliveTask(this.leaseClient,
                                                          leaseId),
                                        period, period, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Thread {} failed to create lease for {} with ttl {}", e,
                     Thread.currentThread().getName(), lockName, ttl);
            return lockResult;
        }

        LOG.debug("Thread {} start to lock {}",
                  Thread.currentThread().getName(), lockName);

        try {
            this.lockClient.lock(toByteSequence(lockName), leaseId).get();
        } catch (InterruptedException | ExecutionException e1) {
            LOG.warn("Thread {} failed to lock {}", e1,
                     Thread.currentThread().getName(), lockName);
            return lockResult;
        }
        LOG.debug("Thread {} lock {} successfully",
                  Thread.currentThread().getName(), lockName);

        lockResult.lockSuccess(true);

        return lockResult;
    }

    public void unLock(String lockName, LockResult lockResult) {
        LOG.debug("Thread {} start to unlock {}",
                  Thread.currentThread().getName(), lockName);
        try {
            this.lockClient.unlock(toByteSequence(lockName)).get();
            lockResult.getService().shutdown();
            if (lockResult.getLeaseId() != 0L) {
                this.leaseClient.revoke(lockResult.getLeaseId());
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Thread {} failed to unlock {}", e,
                     Thread.currentThread().getName(), lockName);
        }

        LOG.debug("Thread {} unlock {} successfully",
                  Thread.currentThread().getName(), lockName);
    }

    private static ByteSequence toByteSequence(String content) {
        return ByteSequence.from(content, Charset.defaultCharset());
    }

    public static class KeepAliveTask implements Runnable {

        private Lease leaseClient;
        private long leaseId;

        KeepAliveTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run() {
            this.leaseClient.keepAliveOnce(this.leaseId);
        }
    }
}
