package com.baidu.hugegraph2.type;

public enum HugeTypes {

    UNKNOWN(0),

    // schema types
    VERTEX_LABEL(1),
    EDGE_LABEL(2),
    PROPERTY_KEY(3),

    // data types
    VERTEX(101),
    EDGE_IN(102), // edge's direction is IN for the specified vertex
    EDGE_OUT(103), // edge's direction is OUT for the specified vertex
    PROPERTY(104),
    VERTEX_PROPERTY(105),

    // system meta
    SYS_PROPERTY(255);

    // HugeType define
    private byte type = 0;

    private HugeTypes(int type) {
        assert type < 256;
        this.type = (byte) type;
    }

    public byte code() {
        return this.type;
    }
}
