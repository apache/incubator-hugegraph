package com.baidu.hugegraph.store.node.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * Table Key pair.
 */
public class TkEntry {
    private String table;
    private byte[] key;

    public TkEntry(String table, byte[] key) {
        this.table = table;
        this.key = key;
    }

    public String getTable() {
        return table;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TkEntry)) return false;
        TkEntry tk = (TkEntry) o;
        return Objects.equals(table, tk.table) && Arrays.equals(key, tk.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }

    @Override
    public String toString() {
        return "Tk{" +
                "table='" + table + '\'' +
                ", key=" + Arrays.toString(key) +
                '}';
    }
}
