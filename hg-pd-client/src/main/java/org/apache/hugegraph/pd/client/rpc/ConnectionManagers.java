package org.apache.hugegraph.pd.client.rpc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.PulseClient;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2024/1/31
 **/
@Slf4j
public class ConnectionManagers {

    private static final ConnectionManagers INSTANCE = new ConnectionManagers();
    private static ConcurrentMap<String, ConnectionManager> cms = new ConcurrentHashMap<>();

    public static ConnectionManagers getInstance() {
        return INSTANCE;
    }

    public synchronized ConnectionManager add(PDConfig config) {
        String pds = config.getServerHost();
        String[] hosts = pds.split(",");
        ConnectionManager manager = null;
        if (hosts.length > 0 && !StringUtils.isEmpty(hosts[0])) {
            manager = cms.get(hosts[0]);
            if (manager == null) {
                manager = new ConnectionManager(config);
                cms.put(hosts[0], manager);
                PulseClient pulseClient = new PulseClient(config);
                ConnectionClient connectionClient = new ConnectionClient(config);
                manager.init(pulseClient, connectionClient);
            }
            for (int i = 1; i < hosts.length; i++) {
                cms.putIfAbsent(hosts[i], manager);
            }
        }
        return manager;
    }

    public ConnectionManager get(String host) {
        return cms.get(host);
    }

    public ConnectionManager get(PDConfig config) {
        String pds = config.getServerHost();
        String[] hosts = pds.split(",");
        for (String host : hosts) {
            ConnectionManager manager = cms.get(host);
            if (manager != null) {
                return manager;
            }
        }
        return null;
    }

    public void reset(String leader) {
        ConnectionManager manager = cms.get(leader);
        if (manager == null) {
            return;
        }
        manager.reconnect(leader, false);
    }

    public void resetPeers(String[] peers) {
        for (String peer : peers) {
            ConnectionManager manager = cms.get(peer);
            if (manager == null) {
                continue;
            }
            manager.updatePeers(peers);
            break;
        }
    }
}
