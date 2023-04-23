package com.baidu.hugegraph.store.client.grpc;

import com.baidu.hugegraph.store.HgKvEntry;

import java.util.Arrays;

/**
 * @author lynn.bond@hotmail.com
 */
class GrpcKvEntryImpl implements HgKvEntry {
    private byte[] key;
    private byte[] value;
    private int code;

    GrpcKvEntryImpl(byte[] k, byte[] v,int code) {
        this.key = k;
        this.value = v;
        this.code=code;
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GrpcKvEntryImpl hgKvEntry = (GrpcKvEntryImpl) o;
        return Arrays.equals(key, hgKvEntry.key) && Arrays.equals(value, hgKvEntry.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "HgKvEntryImpl{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", code=" + code +
                '}';
    }
}