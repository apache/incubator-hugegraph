package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgSessionProvider;
import com.baidu.hugegraph.store.HgStoreSession;

import javax.annotation.concurrent.ThreadSafe;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
@ThreadSafe
public class HgStoreSessionProvider implements HgSessionProvider {
    private MultiNodeSessionFactory sessionFactory=MultiNodeSessionFactory.getInstance();

    @Override
    public HgStoreSession createSession(String graphName) {
        return this.sessionFactory.createStoreSession(graphName);
    }
}
