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

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.AbstractClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.K;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.Kv;
import org.apache.hugegraph.pd.grpc.kv.KvResponse;
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.LockRequest;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.TTLRequest;
import org.apache.hugegraph.pd.grpc.kv.TTLResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchRequest;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchType;

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KvClient<T extends WatchResponse> extends AbstractClient implements Closeable {

    private final AtomicLong clientId = new AtomicLong(0);
    private final Semaphore semaphore = new Semaphore(1);
    private final ConcurrentHashMap<Long, StreamObserver> observers = new ConcurrentHashMap<>();

    public KvClient(PDConfig pdConfig) {
        super(pdConfig);
    }

    @Override
    protected AbstractStub createStub() {
        return KvServiceGrpc.newStub(channel);
    }

    @Override
    protected AbstractBlockingStub createBlockingStub() {
        return KvServiceGrpc.newBlockingStub(channel);
    }

    public KvResponse put(String key, String value) throws PDException {
        Kv kv = Kv.newBuilder().setKey(key).setValue(value).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getPutMethod(), kv);
        handleErrors(response.getHeader());
        return response;
    }


    public KResponse get(String key) throws PDException {
        K k = K.newBuilder().setKey(key).build();
        KResponse response = blockingUnaryCall(KvServiceGrpc.getGetMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }


    public KvResponse delete(String key) throws PDException {
        K k = K.newBuilder().setKey(key).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getDeleteMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }


    public KvResponse deletePrefix(String prefix) throws PDException {
        K k = K.newBuilder().setKey(prefix).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getDeletePrefixMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }


    public ScanPrefixResponse scanPrefix(String prefix) throws PDException {
        K k = K.newBuilder().setKey(prefix).build();
        ScanPrefixResponse response = blockingUnaryCall(KvServiceGrpc.getScanPrefixMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public TTLResponse keepTTLAlive(String key) throws PDException {
        TTLRequest request = TTLRequest.newBuilder().setKey(key).build();
        TTLResponse response = blockingUnaryCall(KvServiceGrpc.getKeepTTLAliveMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    public TTLResponse putTTL(String key, String value, long ttl) throws PDException {
        TTLRequest request =
                TTLRequest.newBuilder().setKey(key).setValue(value).setTtl(ttl).build();
        TTLResponse response = blockingUnaryCall(KvServiceGrpc.getPutTTLMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    private void onEvent(WatchResponse value, Consumer<T> consumer) {
        log.info("receive message for {},event Count:{}", value, value.getEventsCount());
        clientId.compareAndSet(0L, value.getClientId());
        if (value.getEventsCount() != 0) {
            consumer.accept((T) value);
        }
    }

    private StreamObserver<WatchResponse> getObserver(String key, Consumer<T> consumer,
                                                      BiConsumer<String, Consumer> listenWrapper,
                                                      long client) {
        StreamObserver<WatchResponse> observer;
        if ((observer = observers.get(client)) == null) {
            synchronized (this) {
                if ((observer = observers.get(client)) == null) {
                    observer = getObserver(key, consumer, listenWrapper);
                    observers.put(client, observer);
                }
            }
        }
        return observer;
    }

    private StreamObserver<WatchResponse> getObserver(String key, Consumer<T> consumer,
                                                      BiConsumer<String, Consumer> listenWrapper) {
        return new StreamObserver<WatchResponse>() {
            @Override
            public void onNext(WatchResponse value) {
                switch (value.getState()) {
                    case Starting:
                        boolean b = clientId.compareAndSet(0, value.getClientId());
                        if (b) {
                            observers.put(value.getClientId(), this);
                            log.info("set watch client id to :{}", value.getClientId());
                        }
                        semaphore.release();
                        break;
                    case Started:
                        onEvent(value, consumer);
                        break;
                    case Leader_Changed:
                        listenWrapper.accept(key, consumer);
                        break;
                    case Alive:
                        // only for check client is alive, do nothing
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                listenWrapper.accept(key, consumer);
            }


            @Override
            public void onCompleted() {

            }
        };
    }

    public void listen(String key, Consumer<T> consumer) throws PDException {
        long value = clientId.get();
        StreamObserver<WatchResponse> observer = getObserver(key, consumer, listenWrapper, value);
        acquire();
        WatchRequest k = WatchRequest.newBuilder().setClientId(value).setKey(key).build();
        streamingCall(KvServiceGrpc.getWatchMethod(), k, observer, 1);
    }

    public void listenPrefix(String prefix, Consumer<T> consumer) throws PDException {
        long value = clientId.get();
        StreamObserver<WatchResponse> observer =
                getObserver(prefix, consumer, prefixListenWrapper, value);
        acquire();
        WatchRequest k =
                WatchRequest.newBuilder().setClientId(clientId.get()).setKey(prefix).build();
        streamingCall(KvServiceGrpc.getWatchPrefixMethod(), k, observer, 1);
    }

    private void acquire() {
        if (clientId.get() == 0L) {
            try {
                semaphore.acquire();
                if (clientId.get() != 0L) {
                    semaphore.release();
                }
            } catch (InterruptedException e) {
                log.error("get semaphore with error:", e);
            }
        }
    }

    public List<String> getWatchList(T response) {
        List<String> values = new LinkedList<>();
        List<WatchEvent> eventsList = response.getEventsList();
        for (WatchEvent event : eventsList) {
            if (event.getType() != WatchType.Put) {
                return null;
            }
            String value = event.getCurrent().getValue();
            values.add(value);
        }
        return values;
    }

    public Map<String, String> getWatchMap(T response) {
        Map<String, String> values = new HashMap<>();
        List<WatchEvent> eventsList = response.getEventsList();
        for (WatchEvent event : eventsList) {
            if (event.getType() != WatchType.Put) {
                return null;
            }
            WatchKv current = event.getCurrent();
            String key = current.getKey();
            String value = current.getValue();
            values.put(key, value);
        }
        return values;
    }

    public LockResponse lock(String key, long ttl) throws PDException {
        acquire();
        LockResponse response;
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(clientId.get()).setTtl(ttl)
                               .build();
            response = blockingUnaryCall(KvServiceGrpc.getLockMethod(), k);
            handleErrors(response.getHeader());
            if (clientId.compareAndSet(0L, response.getClientId())) {
                semaphore.release();
            }
        } catch (Exception e) {
            if (clientId.get() == 0L) {
                semaphore.release();
            }
            throw e;
        }
        return response;
    }    BiConsumer<String, Consumer> listenWrapper = (key, consumer) -> {
        try {
            listen(key, consumer);
        } catch (PDException e) {
            try {
                log.warn("start listen with warning:", e);
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
    };

    public LockResponse lockWithoutReentrant(String key, long ttl) throws PDException {
        acquire();
        LockResponse response;
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(clientId.get()).setTtl(ttl)
                               .build();
            response = blockingUnaryCall(KvServiceGrpc.getLockWithoutReentrantMethod(), k);
            handleErrors(response.getHeader());
            if (clientId.compareAndSet(0L, response.getClientId())) {
                semaphore.release();
            }
        } catch (Exception e) {
            if (clientId.get() == 0L) {
                semaphore.release();
            }
            throw e;
        }
        return response;
    }

    public LockResponse isLocked(String key) throws PDException {
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getIsLockedMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public LockResponse unlock(String key) throws PDException {
        assert clientId.get() != 0;
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getUnlockMethod(), k);
        handleErrors(response.getHeader());
        clientId.compareAndSet(0L, response.getClientId());
        assert clientId.get() == response.getClientId();
        return response;
    }

    public LockResponse keepAlive(String key) throws PDException {
        assert clientId.get() != 0;
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getKeepAliveMethod(), k);
        handleErrors(response.getHeader());
        clientId.compareAndSet(0L, response.getClientId());
        assert clientId.get() == response.getClientId();
        return response;
    }

    @Override
    public void close() {
        super.close();
    }



    BiConsumer<String, Consumer> prefixListenWrapper = (key, consumer) -> {
        try {
            listenPrefix(key, consumer);
        } catch (PDException e) {
            try {
                log.warn("start listenPrefix with warning:", e);
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
    };


}
