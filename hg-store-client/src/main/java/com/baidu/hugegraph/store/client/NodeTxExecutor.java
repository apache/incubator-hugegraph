package com.baidu.hugegraph.store.client;

import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.EMPTY_LIST;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.NODE_MAX_RETRYING_TIMES;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.TX_SESSIONS_MAP_CAPACITY;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.type.HgStoreClientException;
import com.baidu.hugegraph.store.client.util.HgAssert;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.term.HgPair;
import com.baidu.hugegraph.store.term.HgTriple;

import lombok.extern.slf4j.Slf4j;


/**
 * @author lynn.bond@hotmail.com on 2021/11/18
 */
@Slf4j
@NotThreadSafe
final class NodeTxExecutor {
    private String graphName;
    private Map<Long, HgStoreSession> sessions = new HashMap<>(TX_SESSIONS_MAP_CAPACITY, 1);
    private boolean isTx;
    NodeTxSessionProxy proxy;
    private List<HgPair<HgTriple<String, HgOwnerKey, Object>,
            Function<NodeTkv, Boolean>>> entries = new LinkedList<>();
    Collector<NodeTkv, ?, Map<Long, List<HgOwnerKey>>> collector = Collectors.groupingBy(
            nkv -> nkv.getNodeId(), Collectors.mapping(NodeTkv::getKey, Collectors.toList()));
    private static String maxTryMsg = "the number of retries reached the upper limit : " + NODE_MAX_RETRYING_TIMES +
                                      ",caused by:";
    private static String msg = "Not all tx-data delivered to real-node-session successfully.";
    static {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism",
                           String.valueOf(Runtime.getRuntime().availableProcessors() * 2));
    }



    static NodeTxExecutor graphOf(String graphName, NodeTxSessionProxy proxy) {
        return new NodeTxExecutor(graphName, proxy);
    }

    private NodeTxExecutor(String graphName, NodeTxSessionProxy proxy) {
        this.graphName = graphName;
        this.proxy = proxy;
    }

    public boolean isTx() {
        return isTx;
    }

    void setTx(boolean tx) {
        isTx = tx;
    }

    void commitTx() {
        if (!this.isTx) {
            throw new IllegalStateException("It's not in tx state");
        }

        this.doCommit();
    }

    void rollbackTx() {
        if (!this.isTx) {
            return;
        }
        try {
            this.sessions.values().stream().filter(HgStoreSession::isTx)
                         .forEach(HgStoreSession::rollback);
        } catch (Throwable t) {
            throw t;
        } finally {
            this.isTx = false;
            this.sessions.clear();
        }
    }

    void doCommit() {
        try {
            this.retryingInvoke(() -> {
                if (this.entries.isEmpty()) {
                    return true;
                }
                AtomicBoolean allSuccess = new AtomicBoolean(true);
                for (HgPair<HgTriple<String, HgOwnerKey, Object>, Function<NodeTkv, Boolean>> e : this.entries) {
                    doAction(e.getKey(), e.getValue());
                }
                if (!allSuccess.get()) {
                    throw HgStoreClientException.of(msg);
                }
                AtomicReference<Throwable> throwable = new AtomicReference<>();
                Collection<HgStoreSession> sessions = this.sessions.values();
                sessions.parallelStream().forEach(e -> {
                    if (e.isTx()) {
                        try {
                            e.commit();
                        } catch (Throwable t) {
                            throwable.compareAndSet(null, t);
                            allSuccess.set(false);
                        }
                    }
                });
                if (!allSuccess.get()) {
                    if (isTx) {
                        try {
                            sessions.stream().forEach(HgStoreSession::rollback);
                        } catch (Exception e) {

                        }
                    }
                    Throwable cause = throwable.get();
                    if (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    if (cause instanceof HgStoreClientException) {
                        throw (HgStoreClientException) cause;
                    }
                    throw HgStoreClientException.of(cause);
                }
                return true;
            });

        } catch (Throwable t) {
            throw t;
        } finally {
            this.isTx = false;
            this.entries = new LinkedList<>();
            this.sessions = new HashMap<>(TX_SESSIONS_MAP_CAPACITY, 1);
        }
    }

    // private Function<HgTriple<String, HgOwnerKey, Object>,
    //        List<HgPair<HgStoreNode, NodeTkv>>> nodeStreamWrapper = nodeParams -> {
    //    if (nodeParams.getZ() == null) {
    //        return this.proxy.getNode(nodeParams.getX(),
    //                                  nodeParams.getY());
    //    } else {
    //        if (nodeParams.getZ() instanceof HgOwnerKey) {
    //            return this.proxy.getNode(nodeParams.getX(),
    //                                      nodeParams.getY(),
    //                                      (HgOwnerKey) nodeParams.getZ());
    //        } if ( nodeParams.getZ() instanceof Integer ){
    //            return this.proxy.doPartition(nodeParams.getX(), (Integer) nodeParams.getZ())
    //                             .stream()
    //                             .map(e -> new NodeTkv(e, nodeParams.getX(), nodeParams.getY(), nodeParams.getY()
    //                             .getKeyCode()))
    //                             .map(
    //                                     e -> new HgPair<>(this.proxy.getStoreNode(e.getNodeId()), e)
    //                                 );
    //        }else {
    //            HgAssert.isTrue(nodeParams.getZ() instanceof byte[],
    //                            "Illegal parameter to get node id");
    //            throw new NotImplementedException();
    //        }
    //    }
    // };

    // private Function<HgTriple<String, HgOwnerKey, Object>,
    //        List<HgPair<HgStoreNode, NodeTkv>>> nodeStreamWrapper = nodeParams -> {
    //    if (nodeParams.getZ() == null) {
    //        return this.proxy.getNode(nodeParams.getX(), nodeParams.getY());
    //    } else {
    //        if (nodeParams.getZ() instanceof HgOwnerKey) {
    //            return this.proxy.getNode(nodeParams.getX(), nodeParams.getY(),
    //                                      (HgOwnerKey) nodeParams.getZ());
    //        }
    //        if (nodeParams.getZ() instanceof Integer) {
    //            Collection<HgNodePartition> nodePartitions = this.proxy.doPartition(nodeParams.getX(),
    //                                                                                (Integer) nodeParams.getZ());
    //            ArrayList<HgPair<HgStoreNode, NodeTkv>> hgPairs = new ArrayList<>(nodePartitions.size());
    //            for (HgNodePartition nodePartition : nodePartitions) {
    //                NodeTkv nodeTkv = new NodeTkv(nodePartition, nodeParams.getX(), nodeParams.getY(),
    //                                              nodeParams.getY().getKeyCode());
    //                hgPairs.add(new HgPair<>(this.proxy.getStoreNode(nodeTkv.getNodeId()), nodeTkv));
    //
    //            }
    //            return hgPairs;
    //        } else {
    //            HgAssert.isTrue(nodeParams.getZ() instanceof byte[], "Illegal parameter to get node id");
    //            throw new RuntimeException("not implemented");
    //        }
    //    }
    // };

    private boolean doAction(HgTriple<String, HgOwnerKey, Object> nodeParams,
                             Function<NodeTkv, Boolean> action) {
        if (nodeParams.getZ() == null) {
            return this.proxy.doAction(nodeParams.getX(), nodeParams.getY(), nodeParams.getY(), action);
        } else {
            if (nodeParams.getZ() instanceof HgOwnerKey) {
                boolean result = this.proxy.doAction(nodeParams.getX(), nodeParams.getY(),
                                                     (HgOwnerKey) nodeParams.getZ(), action);
                return result;
            }
            if (nodeParams.getZ() instanceof Integer) {
                return this.proxy.doAction(nodeParams.getX(), nodeParams.getY(), (Integer) nodeParams.getZ(), action);
            } else {
                HgAssert.isTrue(nodeParams.getZ() instanceof byte[], "Illegal parameter to get node id");
                throw new RuntimeException("not implemented");
            }
        }
    }

    boolean prepareTx(HgTriple<String, HgOwnerKey, Object> nodeParams,
                      Function<NodeTkv, Boolean> sessionMapper) {
        if (this.isTx) {
            return this.entries.add(new HgPair(nodeParams, sessionMapper));
        } else {
            return this.isAllTrue(nodeParams, sessionMapper);
        }
    }

    public HgStoreSession openNodeSession(HgStoreNode node) {
        HgStoreSession res = this.sessions.get(node.getNodeId());
        if (res == null) {
            this.sessions.put(node.getNodeId(), (res = node.openSession(this.graphName)));
        }
        if (this.isTx) {
            res.beginTx();
        }

        return res;
    }

    <R> R limitOne(
            Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> nodeStreamSupplier,
            Function<SessionData<NodeTkv>, R> sessionMapper, R emptyObj) {

        Optional<R> res = retryingInvoke(
                () -> nodeStreamSupplier.get()
                                        .parallel()
                                        .map(
                                                pair -> new SessionData<NodeTkv>(
                                                        openNodeSession(pair.getKey()),
                                                        pair.getValue())
                                        ).map(sessionMapper)
                                        .filter(
                                                r -> isValid(r)
                                        )
                                        .findAny()
                                        .orElseGet(() -> emptyObj)
        );
        return res.orElse(emptyObj);
    }

    <R> List<R> toList(Function<Long, HgStoreNode> nodeFunction
            , List<HgOwnerKey> keyList
            , Function<HgOwnerKey, Stream<NodeTkv>> flatMapper
            , Function<SessionData<List<HgOwnerKey>>, List<R>> sessionMapper) {
        Optional<List<R>> res = retryingInvoke(
                () -> keyList.stream()
                             .flatMap(flatMapper)
                             .collect(collector)
                             .entrySet()
                             .stream()
                             .map(
                                     e -> new SessionData<List<HgOwnerKey>>
                                             (
                                                     openNodeSession(nodeFunction.apply(e.getKey())), e.getValue()
                                             )
                             )
                             .parallel()
                             .map(sessionMapper)
                             .flatMap(
                                     e -> e.stream()
                             )
                             //.distinct()
                             .collect(Collectors.toList())
        );

        return res.orElse(EMPTY_LIST);
    }

    private boolean isAllTrue(HgTriple<String, HgOwnerKey, Object> nodeParams,
                              Function<NodeTkv, Boolean> action) {
        Optional<Boolean> res = retryingInvoke(() -> doAction(nodeParams, action));
        return res.orElse(false);
    }

    boolean isAllTrue(Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> dataSource,
                      Function<SessionData<NodeTkv>, Boolean> action) {
        Optional<Boolean> res = retryingInvoke(
                () -> dataSource.get()
                                .parallel()
                                .map(
                                        pair -> new SessionData<NodeTkv>(
                                                openNodeSession(pair.getKey()),
                                                pair.getValue())
                                ).map(action)
                                .allMatch(Boolean::booleanValue)
        );

        return res.orElse(false);
    }

    boolean ifAnyTrue(Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> nodeStreamSupplier
            , Function<SessionData<NodeTkv>, Boolean> sessionMapper) {

        Optional<Boolean> res = retryingInvoke(
                () -> nodeStreamSupplier.get()
                                        .parallel()
                                        .map(
                                                pair -> new SessionData<NodeTkv>(
                                                        openNodeSession(pair.getKey()),
                                                        pair.getValue())
                                        )
                                        .map(sessionMapper)
                                        .anyMatch(Boolean::booleanValue)
        );

        return res.orElse(false);
    }

    <T> Optional<T> retryingInvoke(Supplier<T> supplier) {
        return IntStream.rangeClosed(0, NODE_MAX_RETRYING_TIMES).boxed()
                        .map(
                                i -> {
                                    T buffer = null;
                                    try {
                                        buffer = supplier.get();
                                    } catch (Throwable t) {
                                        if (i + 1 <= NODE_MAX_RETRYING_TIMES) {
                                            try {
                                                int sleepTime;
                                                // 前三次每隔一秒做一次尝试
                                                if (i < 3) {
                                                    sleepTime = 1;
                                                } else {
                                                    // 后面逐次递增
                                                    sleepTime = i - 1;
                                                }
                                                log.info("Waiting {} seconds " +
                                                         "for the next try.",
                                                         sleepTime);
                                                Thread.sleep(sleepTime * 1000L);
                                            } catch (InterruptedException e) {
                                                log.error("Failed to sleep", e);
                                            }
                                        } else {
                                            log.error(maxTryMsg, t);
                                            throw HgStoreClientException.of(
                                                    t.getMessage(), t);
                                        }
                                    }
                                    return buffer;
                                }
                        )
                        .filter(e -> e != null)
                        .findFirst();

    }

    private boolean isValid(Object obj) {
        if (obj == null) return false;

        if (HgStoreClientConst.EMPTY_BYTES.equals(obj)) {
            return false;
        }

        if (EMPTY_LIST.equals(obj)) {
            return false;
        }

        return true;
    }

    class SessionData<T> {
        HgStoreSession session;
        T data;

        SessionData(HgStoreSession session, T data) {
            this.session = session;
            this.data = data;
        }

    }
}



