package com.baidu.hugegraph.backend.store.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.google.common.base.Preconditions;

public class CassandraStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(
            CassandraStore.class);

    private String keyspace() {
        return this.name();
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        logger.info("CassandraStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name,
                    new CassandraStore.CassandraSchemaStore(keyspace(), name));
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
                    new CassandraStore.CassandraGraphStore(keyspace(), name));
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
                    new CassandraStore.CassandraIndexStore(keyspace(), name));
        }

        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        Preconditions.checkState(
                store instanceof CassandraStore.CassandraIndexStore);
        return store;
    }

    @Override
    public String type() {
        return "cassandra";
    }
}
