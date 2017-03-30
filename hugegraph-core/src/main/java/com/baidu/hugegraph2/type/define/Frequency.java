package com.baidu.hugegraph2.type.define;

/**
 * Created by liningrui on 2017/3/30.
 */
public enum Frequency {
    SINGLE(1),
    MULTIPLE(2);

    // HugeKeys define
    private byte code = 0;

    private Frequency(int code) {
        assert code < 256;
        this.code = (byte) code;
    }

    public byte code() {
        return this.code;
    }

    public String schema() {
        return this.toString().toLowerCase();
    }
}
