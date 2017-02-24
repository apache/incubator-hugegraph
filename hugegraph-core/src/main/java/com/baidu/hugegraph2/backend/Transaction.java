package com.baidu.hugegraph2.backend;

/**
 * Created by jishilei on 17/3/19.
 */
public interface Transaction {

    public void commit() throws BackendException;

    public void rollback() throws BackendException;
}
