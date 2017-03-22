package com.baidu.hugegraph2.backend.tx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.Transaction;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.query.SliceQuery;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;
import com.google.common.base.Preconditions;

public abstract class AbstractTransaction implements Transaction {

    private BackendStore store;

    Map<Id, BackendEntry> additions;
    Set<Id> deletions;

    public AbstractTransaction(BackendStore store) {
        this.store = store;

        this.additions = new HashMap<Id, BackendEntry>();
        this.deletions = new HashSet<Id>();
    }

    public void prepareCommit() {
        // for sub-class preparing data, nothing to do here
    }

    @Override
    public void commit() throws BackendException {
        this.prepareCommit();

        // if an exception occurred, catch in the upper layer and roll back
        store.beginTx();
        store.mutate(additions.values(), deletions);
        store.commitTx();

        this.additions.clear();
        this.deletions.clear();
    }

    @Override
    public void rollback() throws BackendException {
        store.rollbackTx();
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

    public void addEntry(Id id, String colume, Object value) {
        BackendEntry entry = this.additions.getOrDefault(id, null);
        if (entry == null) {
            entry = new BackendEntry(id);

        }
        entry.colume(colume, value);
        addEntry(entry);
    }

    public void removeEntry(Id id) {
        deletions.add(id);
    }

    public List<BackendEntry> getSlice(SliceQuery query) {
        return store.getSlice(query);
    }
}
