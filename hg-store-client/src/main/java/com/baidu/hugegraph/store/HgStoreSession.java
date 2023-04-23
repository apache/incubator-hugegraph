package com.baidu.hugegraph.store;

import com.baidu.hugegraph.store.client.type.HgStoreClientException;

/**
 * @author lynn.bond@hotmail.com
 */
public interface HgStoreSession extends HgKvStore {

    void beginTx();

    /**
     * @throws IllegalStateException  when the tx hasn't been beginning.
     * @throws HgStoreClientException when failed to commit .
     */
    void commit();

    /**
     * @throws IllegalStateException  when the tx hasn't been beginning.
     * @throws HgStoreClientException when failed to rollback.
     */
    void rollback();

    boolean isTx();
}
