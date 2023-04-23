package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.store.grpc.session.FeedbackRes;
import com.baidu.hugegraph.store.raft.RaftClosure;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lynn.bond@hotmail.com on 2022/1/27
 */

abstract class GrpcClosure<V> implements RaftClosure {
    private V result;
    private Map<Integer, Long> leaderMap = new HashMap<>();

    public V getResult() {
        return result;
    }

    public Map<Integer, Long> getLeaderMap() {
        return leaderMap;
    }

    @Override
    public void onLeaderChanged(Integer partId, Long storeId) {
        leaderMap.put(partId, storeId);
    }

    public void setResult(V result) {
        this.result = result;
    }

    /**
     * 设置输出结果给raftClosure，对于Follower来说，raftClosure为空
     */
    public static <V> void setResult(RaftClosure raftClosure, V result) {
        GrpcClosure closure = (GrpcClosure) raftClosure;
        if (closure != null)
            closure.setResult(result);
    }

    public static <V> RaftClosure newRaftClosure(StreamObserver<V> observer) {
        BatchGrpcClosure<V> wrap = new BatchGrpcClosure<V>(0);
        return wrap.newRaftClosure(s -> {
            wrap.waitFinish(observer, r -> {
                return (V) wrap.selectError((List<FeedbackRes>) r);
            }, 0);
        });
    }
}