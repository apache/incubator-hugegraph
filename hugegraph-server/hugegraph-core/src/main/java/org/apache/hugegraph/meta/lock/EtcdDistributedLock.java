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

package org.apache.hugegraph.meta.lock;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;

public class EtcdDistributedLock {

    protected static final Logger LOG = Log.logger(EtcdDistributedLock.class);
    private static final long UNLIMITED_TIMEOUT = -1L;
    private final static Object mutex = new Object();
    private static EtcdDistributedLock lockProvider = null;
    private final KV kvClient;
    private final Lock lockClient;
    private final Lease leaseClient;

    private static final int poolSize = 8;
    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(poolSize, r -> {
        Thread t = new Thread(r, "keepalive");
        t.setDaemon(true);
        return t;
    });

    private EtcdDistributedLock(Client client) {
        this.kvClient = client.getKVClient();
        this.lockClient = client.getLockClient();
        this.leaseClient = client.getLeaseClient();
    }

    public static EtcdDistributedLock getInstance(Client client) {
        synchronized (mutex) {
            if (null == lockProvider) {
                lockProvider = new EtcdDistributedLock(client);
            }
        }
        return lockProvider;
    }

    private static ByteSequence toByteSequence(String content) {
        return ByteSequence.from(content, Charset.defaultCharset());
    }

    public LockResult tryLock(String lockName, long ttl, long timeout) {
        LockResult lockResult = new LockResult();
        lockResult.lockSuccess(false);
        lockResult.setService(service);

        long leaseId;

        try {
            leaseId = this.leaseClient.grant(ttl).get().getID();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn(String.format("Thread {} failed to create lease for {} " +
                                   "with ttl {}", Thread.currentThread().getName(),
                                   lockName, ttl),
                     e);
            return lockResult;
        }

        lockResult.setLeaseId(leaseId);

        long period = ttl - ttl / 5;
        service.scheduleAtFixedRate(new KeepAliveTask(this.leaseClient, leaseId),
                                    period, period, TimeUnit.SECONDS);

        try {
            if (timeout == UNLIMITED_TIMEOUT) {
                this.lockClient.lock(toByteSequence(lockName), leaseId).get();

            } else {
                this.lockClient.lock(toByteSequence(lockName), leaseId)
                               .get(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn(String.format("Thread {} failed to lock {}",
                                   Thread.currentThread().getName(), lockName),
                     e);
            service.shutdown();
            this.revokeLease(leaseId);
            return lockResult;
        } catch (TimeoutException e) {
            // 获取锁超时
            LOG.warn("Thread {} timeout to lock {}",
                     Thread.currentThread().getName(), lockName);
            service.shutdown();
            this.revokeLease(leaseId);
            return lockResult;
        }

        lockResult.lockSuccess(true);

        return lockResult;
    }

    public LockResult lock(String lockName, long ttl) {
        return tryLock(lockName, ttl, UNLIMITED_TIMEOUT);
    }

    public void unLock(String lockName, LockResult lockResult) {
        LOG.debug("Thread {} start to unlock {}",
                  Thread.currentThread().getName(), lockName);

        lockResult.getService().shutdown();

        if (lockResult.getLeaseId() != 0L) {
            this.revokeLease(lockResult.getLeaseId());
        }

        LOG.debug("Thread {} unlock {} successfully",
                  Thread.currentThread().getName(), lockName);
    }

    private void revokeLease(long leaseId) {
        try {
            this.leaseClient.revoke(leaseId).get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn(String.format("Thread %s failed to revoke release %s",
                                   Thread.currentThread().getName(), leaseId), e);
        }
    }

    public static class KeepAliveTask implements Runnable {

        private final Lease leaseClient;
        private final long leaseId;

        KeepAliveTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run() {
            // TODO: calculate the time interval between the calls
            this.leaseClient.keepAliveOnce(this.leaseId);
        }
    }
}
