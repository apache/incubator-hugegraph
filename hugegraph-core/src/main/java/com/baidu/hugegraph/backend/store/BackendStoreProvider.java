package com.baidu.hugegraph.backend.store;

public interface BackendStoreProvider {

    public String name();

    public BackendStore open(String name);

    public void close();

    public void init();

    public void clear();
}
