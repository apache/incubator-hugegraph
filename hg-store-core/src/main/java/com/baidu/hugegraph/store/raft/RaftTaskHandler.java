package com.baidu.hugegraph.store.raft;

import com.baidu.hugegraph.store.util.HgStoreException;

/**
 * 接收raft发送的数据
 */
public interface RaftTaskHandler {
    boolean invoke(final int groupId, final byte[] request, RaftClosure response) throws HgStoreException;
    boolean invoke(final int groupId, final byte methodId, final Object req, RaftClosure response) throws HgStoreException;
}
