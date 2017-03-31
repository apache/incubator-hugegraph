package com.baidu.hugegraph2.backend.serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;

public class BinaryBackendEntry implements BackendEntry {

    private Id id;
    private Collection<BackendColumn> columns;

    public BinaryBackendEntry(Id id) {
        this.id = id;
        this.columns = new ArrayList<>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public void id(Id id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    public BackendColumn column(byte[] name) {
        for (BackendColumn col : this.columns) {
            if (Arrays.equals(col.name, name)) {
                return col;
            }
        }
        return null;
    }

    public void column(BackendColumn column) {
        this.columns.add(column);
    }

    @Override
    public Collection<BackendColumn> columns() {
        return this.columns;
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns = bytesColumns;
    }
}
