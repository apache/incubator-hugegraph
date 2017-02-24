package com.baidu.hugegraph2.backend.store;

import java.util.Map;

/**
 * Created by jishilei on 17/3/19.
 */
public class DBEntry {

    private Object id;
    private Map<String, Object> columns;

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }
}
