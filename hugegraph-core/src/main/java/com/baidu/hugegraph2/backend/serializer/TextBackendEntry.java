package com.baidu.hugegraph2.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.util.StringEncoding;

public class TextBackendEntry implements BackendEntry {

    private Id id;
    private Map<String, String> columns;

    public TextBackendEntry(Id id) {
        this.id = id;
        this.columns = new HashMap<String, String>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public void id(Id id) {
        this.id = id;
    }

    public Set<String> columnNames() {
        return this.columns.keySet();
    }

    public void column(String column, String value) {
        this.columns.put(column, value);
    }

    public String column(String colume) {
        return this.columns.get(colume);
    }

    public boolean contains(String colume) {
        return this.columns.containsKey(colume);
    }

    public boolean contains(String colume,String value) {
        return this.columns.containsKey(colume) && this.columns.get(colume).equals(value);
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> list = new ArrayList<BackendColumn>(this.columns.size());
        for (Entry<String, String> column : this.columns.entrySet()) {
            BackendColumn bytesColumn = new BackendColumn();
            bytesColumn.name = StringEncoding.encodeString(column.getKey());
            bytesColumn.value = StringEncoding.encodeString(column.getValue());
            list.add(bytesColumn);
        }
        return list;
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns.clear();

        for (BackendColumn column : bytesColumns) {
            this.columns.put(StringEncoding.decodeString(column.name),
                    StringEncoding.decodeString(column.value));
        }
    }
}
