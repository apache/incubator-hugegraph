package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.BackendException;

/**
 * Created by jishilei on 17/3/19.
 */
public interface BackendStoreProvider {

    public BackendStore open(String name) throws BackendException;

    public void close() throws BackendException;

    public void clear() throws BackendException;

    public String name();
}
