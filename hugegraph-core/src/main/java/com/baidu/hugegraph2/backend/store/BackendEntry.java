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

    public Id id() {
        return id;
    }

    public void id(Id id) {
        this.id = id;
    }

    public Map<String, Object> columns() {
        return columns;
    }

    public void columns(Map<String, Object> columns) {
        this.columns = columns;
    }

    public void colume(String colume, Object value) {
        this.columns.put(colume, value);
    }

    public Object colume(String colume) {
        return this.columns.get(colume);
    }

    @Override
    public String toString() {
        return String.format("%s: %s", id, columns.toString());
    }
}
