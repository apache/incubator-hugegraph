package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgTkvEntry;
import com.baidu.hugegraph.store.HgTokvEntry;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/14
 */
class HgTkvEntryImpl implements HgTkvEntry {
    private String table;
    private byte[] key;
    private byte[] value;

    HgTkvEntryImpl(String table, byte[] key, byte[] value) {
        this.table = table;
        this.key = key;
        this.value = value;
    }

    @Override
    public String table() {
        return this.table;
    }

    @Override
    public byte[] key() {
        return this.key;
    }

    @Override
    public byte[] value() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HgTkvEntryImpl that = (HgTkvEntryImpl) o;
        return Objects.equals(table, that.table) && Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "HgTkvEntryImpl{" +
                "table='" + table + '\'' +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
