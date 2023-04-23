package com.baidu.hugegraph.store;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import com.baidu.hugegraph.store.client.HgStoreNodePartitionerImpl;
import com.baidu.hugegraph.store.client.HgStoreSessionProvider;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Maintain HgStoreSession instances.
 * // TODO: Holding more than one HgSessionManager is available,if you want to connect multi HgStore-clusters.
 *
 * @author lynn.bond@hotmail.com
 */

@ThreadSafe
public final class HgStoreClient {
    private HgSessionProvider sessionProvider;
    private PDClient pdClient;

    public HgStoreClient() {
        this.sessionProvider = new HgStoreSessionProvider();
    }

    public HgStoreClient(PDConfig config) {
        this.sessionProvider = new HgStoreSessionProvider();
        pdClient = PDClient.create(config);
        setPdClient(pdClient);
    }
    public HgStoreClient(PDClient pdClient) {
        this.sessionProvider = new HgStoreSessionProvider();
        setPdClient(pdClient);
    }
    public static HgStoreClient create(PDConfig config) {
        return new HgStoreClient(config);
    }

    public static HgStoreClient create(PDClient pdClient) {
        return new HgStoreClient(pdClient);
    }

    public static HgStoreClient create(){
        return new HgStoreClient();
    }

    public void setPDConfig(PDConfig config) {
        pdClient = PDClient.create(config);
        setPdClient(pdClient);
    }

    public void setPdClient(PDClient client) {
        this.pdClient = client;
        HgStoreNodeManager nodeManager =
                HgStoreNodeManager.getInstance();

        HgStoreNodePartitionerImpl p = new HgStoreNodePartitionerImpl(pdClient, nodeManager);
        nodeManager.setNodeProvider(p);
        nodeManager.setNodePartitioner(p);
        nodeManager.setNodeNotifier(p);
    }

    /**
     * Retrieve or create a HgStoreSession.
     *
     * @param graphName
     * @return
     */
    public HgStoreSession openSession(String graphName) {
        return this.sessionProvider.createSession(graphName);
    }

    public PDClient getPdClient(){
        return pdClient;
    }

}
