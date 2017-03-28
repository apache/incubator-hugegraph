package com.baidu.hugegraph2.backend.id;

public interface Id extends Comparable<Id> {

    public abstract String asString();

    public abstract long asLong();

    public abstract byte[] asBytes();
}
