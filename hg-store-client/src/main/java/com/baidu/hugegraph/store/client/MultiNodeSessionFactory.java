package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgStoreSession;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
@ThreadSafe
public final class MultiNodeSessionFactory {
    // TODO multi-instance ?
    private final static MultiNodeSessionFactory instance = new MultiNodeSessionFactory();
    // TODO multi-instance ?
    private HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    // TODO: to be a chain assigned to each graph
    //private HgStoreNodeDispatcher storeNodeDispatcher;

    private MultiNodeSessionFactory() {

        //this.storeNodeDispatcher = new KeyHashStoreNodeDispatcher();
    }

    static MultiNodeSessionFactory getInstance() {
        return instance;
    }

    HgStoreSession createStoreSession(String graphName) {
        return buildProxy(graphName);
    }

    private HgStoreSession buildProxy(String graphName) {
        //return new MultiNodeSessionProxy(graphName, nodeManager, storeNodeDispatcher);
        //return new NodePartitionSessionProxy(graphName,nodeManager);
        //return new NodeRetrySessionProxy(graphName,nodeManager);
        return new NodeTxSessionProxy(graphName, nodeManager);
    }
}
