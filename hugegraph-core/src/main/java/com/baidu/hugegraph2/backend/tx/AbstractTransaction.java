package com.baidu.hugegraph2.backend.tx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.Transaction;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;


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

    public void addEntry(Id id, String colume, Object value) {
        BackendEntry entry = this.additions.getOrDefault(id, null);
        if (entry == null) {
            entry = new BackendEntry(id);
            this.additions.put(id, entry);
        }
        entry.setColume(colume, value);
    }

    public void removeEntry(Id id) {
        deletions.add(id);
    }
}
