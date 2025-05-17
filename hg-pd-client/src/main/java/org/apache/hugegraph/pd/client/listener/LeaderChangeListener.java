package org.apache.hugegraph.pd.client.listener;

import org.apache.hugegraph.pd.client.rpc.ConnectionManagers;

/**
 * @author zhangyingjie
 * @date 2024/1/31
 **/
public interface LeaderChangeListener {
    void onLeaderChanged(String leaderAddress);

    default void onPeerChanged(String[] peers) {
        ConnectionManagers managers = ConnectionManagers.getInstance();
        managers.resetPeers(peers);
    }
}
