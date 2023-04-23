package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgStoreSession;

import java.util.Set;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/11
 * @version 0.2.0
 */
public interface HgStoreNode {

    /**
     * Return boolean value of being online or not
     * @return
     */
    default boolean isHealthy(){
        return true;
    }

    /**
     * Return the unique ID of store-node.
     * @return
     */
    Long getNodeId();

    /**
     * A string value concatenated by host and port: "host:port"
     * @return
     */
    String getAddress();

    /**
     * Return a new HgStoreSession instance, that is not Thread safe.
     * Return null when the node is not in charge of the graph that was passed from argument.
     * @return
     */
    HgStoreSession openSession(String graphName);

}