package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.Transaction;

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
