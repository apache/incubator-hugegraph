package com.baidu.hugegraph.backend.store;

/**
 * Created by liningrui on 2017/7/13.
 */
public enum MutateAction {

    INSERT(0, "insert"),

    APPEND(1, "append"),

    ELIMINATE(2, "eliminate"),

    DELETE(3, "delete");

    private byte code = 0;
    private String name = null;

    private MutateAction(int code, String name) {
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
