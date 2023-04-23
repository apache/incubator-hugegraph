package com.baidu.hugegraph.store.client;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/27
 */
public interface HgStoreNodeProvider {

    /**
     * Applying a new HgStoreNode instance
     * @param graphName
     * @param nodeId
     * @return
     */
    HgStoreNode apply(String graphName, Long nodeId);

}
