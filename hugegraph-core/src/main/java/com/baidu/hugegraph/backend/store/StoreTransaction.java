package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.Transaction;

/**
 * Created by jishilei on 17/3/19.
 */
public class StoreTransaction implements Transaction {

    @Override
    public void commit() throws BackendException {

    }

    @Override
    public void rollback() throws BackendException {

    }
}
