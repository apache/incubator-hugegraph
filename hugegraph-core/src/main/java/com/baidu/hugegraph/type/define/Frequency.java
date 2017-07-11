package com.baidu.hugegraph.type.define;

/**
 * Created by liningrui on 2017/3/30.
 */
public enum Frequency {

    SINGLE(1, "single"),

    MULTIPLE(2, "multiple");

    private byte code = 0;
    private String name = null;

    private Frequency(int code, String name) {
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

    public String schema() {
        String schema = this == SINGLE ?
                        "singleTime" :
                        "multiTimes";
        return String.format(".%s()", schema);
    }
}
