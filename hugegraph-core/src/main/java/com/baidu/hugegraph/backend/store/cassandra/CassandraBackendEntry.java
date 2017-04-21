package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;

public class CassandraBackendEntry implements BackendEntry {

    public static class Cell {
        private HugeKeys nameType;
        private HugeKeys valueType;
        private String name;
        private String value;

        public Cell(HugeKeys nameType, String name,
                HugeKeys valueType, String value) {
            assert nameType != null && name != null;
            this.nameType = nameType;
            this.name = name;
            this.valueType = valueType;
            this.value = value;
        }

        public HugeKeys nameType() {
            return this.nameType;
        }

        public void nameType(HugeKeys nameType) {
            this.nameType = nameType;
        }

        public HugeKeys valueType() {
            return this.valueType;
        }

        public void valueType(HugeKeys valueType) {
            this.valueType = valueType;
        }

        public String name() {
            return this.name;
        }

        public void name(String name) {
            this.name = name;
        }

        public String value() {
            return this.value;
        }

        public void value(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("Cell{%s(%s): %s(%s)}",
                    this.name,
                    this.nameType,
                    this.value,
                    this.valueType);
        }
    }

    public static class Row {
        private HugeTypes type;
        private Id id;
        private Map<HugeKeys, String> keys;
        private List<Cell> cells;

        public Row(HugeTypes type) {
            this(type, null);
        }

        public Row(HugeTypes type, Id id) {
            this.type = type;
            this.id = id;
            this.keys = new ConcurrentHashMap<>();
            this.cells = new LinkedList<>();
        }

        public HugeTypes type() {
            return this.type;
        }

        public Id id() {
            return this.id;
        }

        public void key(HugeKeys key, String value) {
            this.keys.put(key, value);
        }

        public void key(HugeKeys key, int value) {
            this.keys.put(key, String.valueOf(value));
        }

        public String key(HugeKeys key) {
            return this.keys.get(key);
        }

        public Map<HugeKeys, String> keys() {
            return this.keys;
        }

        public void cell(Cell value) {
            this.cells.add(value);
        }

        public List<Cell> cells() {
            return this.cells;
        }

        @Override
        public String toString() {
            return String.format("Row{type=%s, id=%s, keys=%s, cells=%s}",
                    this.type,
                    this.id,
                    this.keys,
                    this.cells);
        }
    }

    private Row row = null;
    private List<Row> subRows = null;

    public CassandraBackendEntry(Id id) {
        this(null, id);
    }

    public CassandraBackendEntry(HugeTypes type) {
        this(type, null);
    }

    public CassandraBackendEntry(HugeTypes type, Id id) {
        this.row = new Row(type, id);
        this.subRows = new ArrayList<>();
    }

    public HugeTypes type() {
        return this.row.type;
    }

    public void type(HugeTypes type) {
        this.row.type = type;
    }

    @Override
    public Id id() {
        return this.row.id;
    }

    @Override
    public void id(Id id) {
        this.row.id = id;
    }

    public Row row() {
        return this.row;
    }

    public Map<HugeKeys, String> keys() {
        return this.row.keys();
    }

    public List<Cell> cells() {
        return this.row.cells();
    }

    public void column(HugeKeys key, String value) {
        this.row.key(key, value);
    }

    public void column(Cell cell) {
        this.row.cell(cell);
    }

    public String column(HugeKeys key) {
        return this.row.key(key);
    }

    public void subRow(Row row) {
        this.subRows.add(row);
    }

    public List<Row> subRows() {
        return this.subRows;
    }

    public void subRows(List<Row> rows) {
        this.subRows = rows;
    }

    @Override
    public String toString() {
        return String.format("CassandraBackendEntry{%s, sub-rows: %s}",
                this.row.toString(),
                this.subRows.toString());
    }

    @Override
    public Collection<BackendColumn> columns() {
        throw new RuntimeException("Not supported by Cassandra");
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        throw new RuntimeException("Not supported by Cassandra");
    }
}
