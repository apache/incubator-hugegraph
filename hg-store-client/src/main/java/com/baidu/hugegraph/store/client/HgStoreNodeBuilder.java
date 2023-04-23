package com.baidu.hugegraph.store.client;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/11
 */
public interface HgStoreNodeBuilder {

    HgStoreNodeBuilder setNodeId(Long nodeId);

    HgStoreNodeBuilder setAddress(String address);

    /**
     * To build a HgStoreNode instance.
     *
     * @return
     */
    HgStoreNode build();

}
