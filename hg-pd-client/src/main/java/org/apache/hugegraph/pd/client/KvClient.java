package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.impl.StreamDelegator;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
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

import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2022/6/20
 **/
@Slf4j
public class KvClient<T extends WatchResponse> extends BaseClient implements Closeable {

    private static String keyListenPrefix = "K-";
    private static String prefixListenPrefix = "P-";
    private static String delegatorPrefix = "KV-";
    private AtomicLong clientId = new AtomicLong(0L);
    private Semaphore semaphore = new Semaphore(1);
    private AtomicBoolean closed = new AtomicBoolean(false);
    private ConcurrentMap<String, StreamDelegator> delegators = new ConcurrentHashMap<>();

    public KvClient(PDConfig pdConfig) {
        super(pdConfig, KvServiceGrpc::newStub, KvServiceGrpc::newBlockingStub);
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
        TTLRequest request = TTLRequest.newBuilder().setKey(key).setValue(value).setTtl(ttl).build();
        TTLResponse response = blockingUnaryCall(KvServiceGrpc.getPutTTLMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    private void onEvent(WatchResponse value, Consumer<T> consumer) {
        // log.info("receive message for {},event Count:{}", value.getState(), value.getEventsCount());
        this.clientId.compareAndSet(0L, value.getClientId());
        if (value.getEventsCount() != 0) {
            try {
                consumer.accept((T) value);
            } catch (Exception e) {
                log.info(
                        "an error occurred while executing the client callback method, which should not " +
                        "have happened.Please check the callback method of the client",
                        e);
            }
        }
    }

    public void listen(String key, Consumer<T> consumer) throws PDException {
        acquire();
        try {
            StreamDelegator delegator = createDelegator(keyListenPrefix + key,
                                                        KvServiceGrpc.getWatchMethod());
            delegator.listen(getWatchRequest(key), getStreamDataHandler(key, consumer));
        } catch (Exception e) {
            release();
            throw new PDException(ErrorType.PD_UNAVAILABLE, e);
        }
    }

    public void listenPrefix(String prefix, Consumer<T> consumer) throws PDException {
        acquire();
        try {
            StreamDelegator delegator = createDelegator(prefixListenPrefix + prefix,
                                                        KvServiceGrpc.getWatchPrefixMethod());
            delegator.listen(getWatchRequest(prefix), getStreamDataHandler(prefix, consumer));
        } catch (Exception e) {
            release();
            throw new PDException(ErrorType.PD_UNAVAILABLE, e);
        }
    }

    private void acquire() {
        if (this.clientId.get() == 0L) {
            try {
                this.semaphore.acquire();
                if (this.clientId.get() != 0L) {
                    this.semaphore.release();
                } else {
                    log.info("wait for client starting....");
                }
            } catch (Exception e) {
                log.error("get semaphore with error:", e);
            }
        }
    }

    private void release() {
        try {
            if (this.semaphore.availablePermits() == 0) {
                this.semaphore.release();
                log.info("listen finished");
            }
        } catch (Exception e) {
            log.warn("release failed:", e);
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
        LockResponse response;
        acquire();
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(this.clientId.get()).setTtl(ttl).build();
            response = blockingUnaryCall(KvServiceGrpc.getLockMethod(), k);
            handleErrors(response.getHeader());
            this.clientId.compareAndSet(0L, response.getClientId());
        } catch (Exception e) {
            throw e;
        } finally {
            release();
        }
        return response;
    }

    public LockResponse lockWithoutReentrant(String key, long ttl) throws PDException {
        LockResponse response;
        acquire();
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(this.clientId.get()).setTtl(ttl).build();
            response = blockingUnaryCall(KvServiceGrpc.getLockWithoutReentrantMethod(), k);
            handleErrors(response.getHeader());
            this.clientId.compareAndSet(0L, response.getClientId());
        } catch (Exception e) {
            throw e;
        } finally {
            release();
        }
        return response;
    }

    public LockResponse isLocked(String key) throws PDException {
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(this.clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getIsLockedMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public LockResponse unlock(String key) throws PDException {
        assert this.clientId.get() != 0L;
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(this.clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getUnlockMethod(), k);
        handleErrors(response.getHeader());
        this.clientId.compareAndSet(0L, response.getClientId());
        assert this.clientId.get() == response.getClientId();
        return response;
    }

    public LockResponse keepAlive(String key) throws PDException {
        assert this.clientId.get() != 0L;
        LockRequest k = LockRequest.newBuilder().setKey(key).setClientId(this.clientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getKeepAliveMethod(), k);
        handleErrors(response.getHeader());
        this.clientId.compareAndSet(0L, response.getClientId());
        assert this.clientId.get() == response.getClientId();
        return response;
    }

    public void close() {
        this.delegators.entrySet().forEach(d -> d.getValue().close());
        this.delegators.clear();
        this.closed.set(true);
        super.close();
    }

    private Consumer<WatchResponse> getStreamDataHandler(String key, Consumer<T> consumer) {
        return value -> {
            boolean b;
            switch (value.getState()) {
                case Starting:
                    b = this.clientId.compareAndSet(0L, value.getClientId());
                    if (b) {
                        log.info("set watch client id to :{}", Long.valueOf(value.getClientId()));
                    }
                    release();
                    break;
                case Started:
                    onEvent(value, consumer);
                    break;
                case Leader_Changed:
                    this.clientId.set(0L);
                    release();
                    onLeaderChanged("");
                    break;
            }
        };
    }

    private void onDelegatorError(Throwable t) {
        release();
        if (!this.closed.get()) {
            this.clientId.set(0L);
        }
    }

    private WatchRequest getWatchRequest(String key) {
        return WatchRequest.newBuilder().setClientId(this.clientId.get()).setKey(key).build();
    }

    private StreamDelegator createDelegator(String name,
                                            MethodDescriptor<WatchRequest, WatchResponse> methodDesc) {
        StreamDelegator delegator =
                new StreamDelegator(delegatorPrefix + name, getLeaderInvoker(), methodDesc);
        this.delegators.put(delegator.getName(), delegator);
        return delegator;
    }

    public void onLeaderChanged(String leader) {
        if (this.closed.get()) {
            return;
        }
        this.delegators.entrySet().parallelStream().forEach(e -> e.getValue().reconnect());
    }
}
