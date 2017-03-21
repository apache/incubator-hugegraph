package com.baidu.hugegraph2.backend.store;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph2.backend.id.Id;

/**
 * Created by jishilei on 17/3/19.
 */
public class BackendEntry {

    private Id id;
    private Map<String, Object> columns;

    public BackendEntry(Id id) {
        this.id = id;
        this.columns = new HashMap<String, Object>();
    }

    public Id getId() {
        return id;
    }

    public void setId(Id id) {
        this.id = id;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }

    public void setColume(String colume, Object value) {
        this.columns.put(colume, value);
    }

    public Object getColume(String colume) {
        return this.columns.get(colume);
    }

    @Override
    public String toString() {
        return String.format("%s: %s", id, columns.toString());
    }
}
