package com.baidu.hugegraph2.backend.tx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.Transaction;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.query.SliceQuery;
import com.baidu.hugegraph2.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph2.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;
import com.google.common.base.Preconditions;

public abstract class AbstractTransaction implements Transaction {

    // parent graph
    protected final HugeGraph graph;
    protected AbstractSerializer serializer;

    protected BackendStore store;

    protected Map<Id, BackendEntry> additions;
    protected Set<Id> deletions;

    public AbstractTransaction(HugeGraph graph, BackendStore store) {
        this.graph = graph;
        this.serializer = this.graph.serializer();

        this.store = store;
        this.additions = new HashMap<Id, BackendEntry>();
        this.deletions = new HashSet<Id>();
    }

    protected void prepareCommit() {
        // for sub-class preparing data, nothing to do here
    }

    @Override
    public void commit() throws BackendException {
        this.prepareCommit();

        // if an exception occurred, catch in the upper layer and roll back
        this.store.beginTx();
        this.store.mutate(this.additions.values(), this.deletions);
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

    public void addEntry(Id id, String column, String value) {
        // add a column to an Entry
        // create a new one if the Entry with `id` not exists
        BackendEntry entry = this.additions.getOrDefault(id, null);
        if (entry == null) {
            entry = new TextBackendEntry(id);
            this.additions.put(id, entry);
        }
        assert (entry instanceof TextBackendEntry);
        ((TextBackendEntry) entry).column(column, value);
    }

    public void removeEntry(Id id) {
        this.deletions.add(id);
    }

    public List<BackendEntry> getSlice(SliceQuery query) {
        return this.store.getSlice(query);
    }
}
