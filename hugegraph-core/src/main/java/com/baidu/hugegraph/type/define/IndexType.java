package com.baidu.hugegraph.type.define;

/**
 * Created by liningrui on 2017/3/21.
 */
public enum IndexType {

    SECONDARY(1, "secondary"),

    RANGE(2, "range"),

    SEARCH(3, "search");

    private byte code = 0;
    private String name = null;

    private IndexType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
