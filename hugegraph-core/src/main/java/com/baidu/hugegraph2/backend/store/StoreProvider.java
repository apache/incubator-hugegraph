package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.BackendException;

import java.util.Map;


/**
 * Created by jishilei on 17/3/19.
 */
public interface StoreProvider {

    public DBStore open(String name) throws BackendException;

    public void mutate(Map<String , StoreMutation> mutations, StoreTransaction tx);

    public void close() throws BackendException;

    public void clear() throws BackendException;

    public String getName();
}
