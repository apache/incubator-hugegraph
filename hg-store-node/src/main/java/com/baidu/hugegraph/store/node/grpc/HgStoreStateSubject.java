package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.store.grpc.state.NodeStateRes;
import com.baidu.hugegraph.store.grpc.state.NodeStateType;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.baidu.hugegraph.store.node.util.HgAssert.isArgumentNotNull;
import static com.baidu.hugegraph.store.node.util.HgAssert.isArgumentValid;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/3
 */
@Slf4j
public final class HgStoreStateSubject {
    public final static Map<String, StreamObserver<NodeStateRes>> subObserverHolder =
            new ConcurrentHashMap<>();


    public static void addObserver(String subId, StreamObserver<NodeStateRes> observer) {
        isArgumentValid(subId, "subId");
        isArgumentNotNull(observer == null, "observer");

        subObserverHolder.put(subId, observer);
    }

    public static void removeObserver(String subId) {
        isArgumentValid(subId, "subId");
        subObserverHolder.remove(subId);
    }

    public static void notifyAll(NodeStateType nodeState) {

        isArgumentNotNull(nodeState == null, "nodeState");
        NodeStateRes res = NodeStateRes.newBuilder().setState(nodeState).build();
        Iterator<Map.Entry<String, StreamObserver<NodeStateRes>>> iter = subObserverHolder.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, StreamObserver<NodeStateRes>> entry = iter.next();

            try {
                entry.getValue().onNext(res);
            } catch (Throwable e) {
                log.error("Failed to send node-state[" + nodeState + "] to subscriber[" + entry.getKey() + "].", e);
                iter.remove();
                log.error("Removed the subscriber[" + entry.getKey() + "].", e);
            }

        }
    }
}
