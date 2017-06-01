package com.baidu.hugegraph.backend;

public interface Transaction {

    public void commit() throws BackendException;

    public void rollback() throws BackendException;

    public boolean autoCommit();

    public void beforeWrite();

    public void afterWrite();

    public void beforeRead();

    public void afterRead();

    public void close();
}
