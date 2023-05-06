package org.apache.hugegraph.pd.raft;

import com.baidu.hugegraph.pd.common.PDException;

/**
 * 接收raft发送的数据
 */
public interface RaftTaskHandler {
    boolean invoke(final KVOperation op, KVStoreClosure response) throws PDException;
}
