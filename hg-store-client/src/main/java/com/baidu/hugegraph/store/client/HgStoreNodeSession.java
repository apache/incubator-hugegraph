package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgStoreSession;

/**
 * @version 0.1.0
 * @author lynn.bond@hotmail.com created on 2021/10/11
 */
public interface HgStoreNodeSession extends HgStoreSession {

    /**
     * Return the name of graph.
     * @return
     */
    String getGraphName();

    /**
     * Return an instance of HgStoreNode, which provided the connection of Store-Node machine.
     * @return
     */
    HgStoreNode getStoreNode();


}
