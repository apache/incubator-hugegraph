package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgTokvEntry;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/14
 */
class HgTokvEntryImpl implements HgTokvEntry {

    private String table;
    private HgOwnerKey ownerKey;
    private byte[] value;

    HgTokvEntryImpl(String table, HgOwnerKey ownerKey, byte[] value) {
        this.table = table;
        this.ownerKey = ownerKey;
        this.value = value;
    }

    @Override
    public String table() {
        return this.table;
    }

    @Override
    public HgOwnerKey ownerKey() {
        return this.ownerKey;
    }

    @Override
    public byte[] value() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HgTokvEntryImpl that = (HgTokvEntryImpl) o;
        return Objects.equals(table, that.table) && Objects.equals(ownerKey, that.ownerKey) && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table, ownerKey);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "HgTokvEntryImpl{" +
                "table='" + table + '\'' +
                ", okv=" + ownerKey +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
