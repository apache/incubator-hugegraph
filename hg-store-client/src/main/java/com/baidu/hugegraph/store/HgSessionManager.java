package com.baidu.hugegraph.store;

import com.baidu.hugegraph.store.client.HgStoreSessionProvider;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Maintain HgStoreSession instances.
 * // TODO: Holding more than one HgSessionManager is available,if you want to connect multi HgStore-clusters.
 *
 * @author lynn.bond@hotmail.com
 */

@ThreadSafe
public final class HgSessionManager {
    private HgSessionProvider sessionProvider;
    private final static HgSessionManager instance =new HgSessionManager();

    private HgSessionManager(){
        // TODO: constructed by SPI
        this.sessionProvider=new HgStoreSessionProvider();
    }


    public static HgSessionManager getInstance(){
        return instance;
    }

    /**
     * Retrieve or create a HgStoreSession.
     * @param graphName
     * @return
     */
    public HgStoreSession openSession(String graphName){
        return this.sessionProvider.createSession(graphName);
    }

}
