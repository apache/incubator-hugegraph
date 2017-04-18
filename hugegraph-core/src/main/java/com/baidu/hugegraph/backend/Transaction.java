package com.baidu.hugegraph.backend;

/**
 * Created by jishilei on 17/3/19.
 */
public interface Transaction {

    public void commit() throws BackendException;

    public void rollback() throws BackendException;
}
