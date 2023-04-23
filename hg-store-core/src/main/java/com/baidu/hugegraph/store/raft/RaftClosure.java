package com.baidu.hugegraph.store.raft;

import com.alipay.sofa.jraft.Closure;

public interface RaftClosure extends Closure {
    default void onLeaderChanged(Integer partId, Long storeId){}

}
