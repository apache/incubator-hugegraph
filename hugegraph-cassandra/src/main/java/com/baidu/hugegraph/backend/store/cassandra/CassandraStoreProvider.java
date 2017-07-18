package com.baidu.hugegraph.backend.store.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraGraphStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraIndexStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraSchemaStore;
import com.baidu.hugegraph.util.E;

public class CassandraStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger logger =
            LoggerFactory.getLogger(CassandraStore.class);

    private String keyspace() {
        return this.name();
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        logger.info("CassandraStoreProvider load SchemaStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.put(name, new CassandraSchemaStore(this, keyspace(), name));
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraSchemaStore,
                     "SchemaStore must be a instance of CassandraSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        logger.info("CassandraStoreProvider load GraphStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.put(name, new CassandraGraphStore(this, keyspace(), name));
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraGraphStore,
                     "GraphStore must be a instance of CassandraGraphStore");
        return store;
    }

    @Override
    public BackendStore loadIndexStore(String name) {
        logger.info("CassandraStoreProvider load IndexStore '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.put(name, new CassandraIndexStore(this, keyspace(), name));
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof CassandraStore.CassandraIndexStore,
                     "IndexStore must be a instance of CassandraIndexStore");
        return store;
    }

    @Override
    public String type() {
        return "cassandra";
    }
}
