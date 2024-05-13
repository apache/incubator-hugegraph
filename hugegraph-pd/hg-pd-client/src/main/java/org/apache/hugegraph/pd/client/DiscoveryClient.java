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

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.discovery.DiscoveryServiceGrpc;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

@Useless("discovery related")
@Slf4j
public abstract class DiscoveryClient implements Closeable, Discoverable {

    private final Timer timer = new Timer("serverHeartbeat", true);
    private final AtomicBoolean requireResetStub = new AtomicBoolean(false);
    protected int period;
    LinkedList<String> pdAddresses = new LinkedList<>();
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private volatile int currentIndex;
    private int maxTime = 6;
    private ManagedChannel channel = null;
    private DiscoveryServiceGrpc.DiscoveryServiceBlockingStub registerStub;
    private DiscoveryServiceGrpc.DiscoveryServiceBlockingStub blockingStub;

    public DiscoveryClient(String centerAddress, int delay) {
        String[] addresses = centerAddress.split(",");
        for (int i = 0; i < addresses.length; i++) {
            String singleAddress = addresses[i];
            if (singleAddress == null || singleAddress.length() <= 0) {
                continue;
            }
            pdAddresses.add(addresses[i]);
        }
        this.period = delay;
        if (maxTime < addresses.length) {
            maxTime = addresses.length;
        }
    }

    private <V, R> R tryWithTimes(Function<V, R> function, V v) {
        R r;
        Exception ex = null;
        for (int i = 0; i < maxTime; i++) {
            try {
                r = function.apply(v);
                return r;
            } catch (Exception e) {
                requireResetStub.set(true);
                resetStub();
                ex = e;
            }
        }
        if (ex != null) {
            log.error("Try discovery method with error: {}", ex.getMessage());
        }
        return null;
    }

    private void resetStub() {
        String errLog = null;
        for (int i = currentIndex + 1; i <= pdAddresses.size() + currentIndex; i++) {
            currentIndex = i % pdAddresses.size();
            String singleAddress = pdAddresses.get(currentIndex);
            try {
                if (requireResetStub.get()) {
                    resetChannel(singleAddress);
                }
                errLog = null;
                break;
            } catch (Exception e) {
                requireResetStub.set(true);
                if (errLog == null) {
                    errLog = e.getMessage();
                }
                continue;
            }
        }
        if (errLog != null) {
            log.error(errLog);
        }
    }

    private void resetChannel(String singleAddress) throws PDException {

        readWriteLock.writeLock().lock();
        try {
            if (requireResetStub.get()) {
                while (channel != null && !channel.shutdownNow().awaitTermination(
                        100, TimeUnit.MILLISECONDS)) {
                    continue;
                }
                channel = ManagedChannelBuilder.forTarget(
                        singleAddress).usePlaintext().build();
                this.registerStub = DiscoveryServiceGrpc.newBlockingStub(
                        channel);
                this.blockingStub = DiscoveryServiceGrpc.newBlockingStub(
                        channel);
                requireResetStub.set(false);
            }
        } catch (Exception e) {
            throw new PDException(-1, String.format(
                    "Reset channel with error : %s.", e.getMessage()));
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /***
     * Obtain the registration node information
     * @param query
     * @return
     */
    @Override
    public NodeInfos getNodeInfos(Query query) {
        return tryWithTimes((q) -> {
            this.readWriteLock.readLock().lock();
            NodeInfos nodes;
            try {
                nodes = this.blockingStub.getNodes(q);
            } catch (Exception e) {
                throw e;
            } finally {
                this.readWriteLock.readLock().unlock();
            }
            return nodes;
        }, query);
    }

    /***
     * Start the heartbeat task
     */
    @Override
    public void scheduleTask() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                NodeInfo nodeInfo = getRegisterNode();
                tryWithTimes((t) -> {
                    RegisterInfo register;
                    readWriteLock.readLock().lock();
                    try {
                        register = registerStub.register(t);
                        log.debug("Discovery Client work done.");
                        Consumer<RegisterInfo> consumer = getRegisterConsumer();
                        if (consumer != null) {
                            consumer.accept(register);
                        }
                    } catch (Exception e) {
                        throw e;
                    } finally {
                        readWriteLock.readLock().unlock();
                    }
                    return register;
                }, nodeInfo);
            }
        }, 0, period);
    }

    abstract NodeInfo getRegisterNode();

    abstract Consumer<RegisterInfo> getRegisterConsumer();

    @Override
    public void cancelTask() {
        this.timer.cancel();
    }

    @Override
    public void close() {
        this.timer.cancel();
        readWriteLock.writeLock().lock();
        try {
            while (channel != null && !channel.shutdownNow().awaitTermination(
                    100, TimeUnit.MILLISECONDS)) {
                continue;
            }
        } catch (Exception e) {
            log.info("Close channel with error : {}.", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
