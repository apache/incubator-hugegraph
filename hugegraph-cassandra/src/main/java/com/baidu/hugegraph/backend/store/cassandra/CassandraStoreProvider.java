package com.baidu.hugegraph.backend.store.cassandra;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.google.common.base.Preconditions;

public class CassandraStoreProvider implements BackendStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(CassandraStoreProvider.class);

    private String keyspace;
    private ConcurrentHashMap<String, BackendStore> stores;

    @Override
    public BackendStore loadSchemaStore(final String name) {
        logger.info("CassandraStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name,
                    new CassandraStore.CassandraSchemaStore(this.keyspace, name));
        }

        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        Preconditions.checkState(
                store instanceof CassandraStore.CassandraSchemaStore);
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        logger.info("CassandraStoreProvider load GraphStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name,
                    new CassandraStore.CassandraGraphStore(this.keyspace, name));
        }

        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        Preconditions.checkState(
                store instanceof CassandraStore.CassandraGraphStore);
        return store;
    }

    @Override
    public BackendStore loadIndexStore(String name) {
        logger.info("CassandraStoreProvider load IndexStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name,
                    new CassandraStore.CassandraIndexStore(this.keyspace, name));
        }

        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        Preconditions.checkState(
                store instanceof CassandraStore.CassandraIndexStore);
        return store;
    }

    @Override
    public void open(String name) {
        Preconditions.checkNotNull(name);

        this.keyspace = name;
        this.stores = new ConcurrentHashMap<String, BackendStore>();
    }

    @Override
    public void close() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.close();
        }
    }

    @Override
    public void init() {
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
    }

    @Override
    public void clear() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.clear();
        }
    }
    @Override
    public String type() {
        return "cassandra";
    }

    @Override
    public String name() {
        return this.keyspace;
    }
}
