package com.baidu.hugegraph.backend.tx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.Transaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.google.common.base.Preconditions;

public abstract class AbstractTransaction implements Transaction {

    // parent graph
    protected final HugeGraph graph;
    protected AbstractSerializer serializer;
    protected IdGenerator idGenerator;

    protected BackendStore store;

    protected Map<Id, BackendEntry> additions;
    protected Map<Id, BackendEntry> deletions;

    public AbstractTransaction(HugeGraph graph, BackendStore store) {
        this.graph = graph;
        this.serializer = this.graph.serializer();
        this.idGenerator = IdGeneratorFactory.generator();

        this.store = store;
        this.additions = new ConcurrentHashMap<Id, BackendEntry>();
        this.deletions = new ConcurrentHashMap<Id, BackendEntry>();
    }

    public Iterable<BackendEntry> query(Query query) {
        return this.store.query(query);
    }

    public BackendEntry get(Id id) {
        return this.store.get(id);
    }

    public void delete(Id id) {
        this.store.delete(id);
    }

    protected void prepareCommit() {
        // for sub-class preparing data, nothing to do here
    }

    @Override
    public void commit() throws BackendException {
        this.prepareCommit();

        BackendMutation m = new BackendMutation(
                this.additions.values(),
                this.deletions.values());

        // if an exception occurred, catch in the upper layer and roll back
        this.store.beginTx();
        this.store.mutate(m);
        this.store.commitTx();

        this.additions.clear();
        this.deletions.clear();
    }

    @Override
    public void rollback() throws BackendException {
        this.store.rollbackTx();
    }

    public void addEntry(BackendEntry entry) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkNotNull(entry.id());

        Id id = entry.id();
        if (this.additions.containsKey(id)) {
            this.additions.remove(id);
        }
        this.additions.put(id, entry);
    }

    public void removeEntry(BackendEntry entry) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkNotNull(entry.id());

        Id id = entry.id();
        if (this.deletions.containsKey(id)) {
            this.deletions.remove(id);
        }
        this.deletions.put(id, entry);
    }

    public void removeEntry(Id id) {
        this.removeEntry(this.serializer.writeId(id));
    }

}
