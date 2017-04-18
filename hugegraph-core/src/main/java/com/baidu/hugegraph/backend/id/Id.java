package com.baidu.hugegraph.backend.id;

public interface Id extends Comparable<Id> {

    public abstract String asString();

    public abstract long asLong();

    public abstract byte[] asBytes();
}
