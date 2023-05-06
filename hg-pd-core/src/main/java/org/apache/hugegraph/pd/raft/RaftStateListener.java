package org.apache.hugegraph.pd.raft;

public interface RaftStateListener {
    void onRaftLeaderChanged();
}
