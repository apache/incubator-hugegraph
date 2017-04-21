package com.baidu.hugegraph.type;

public enum HugeTypes {

    UNKNOWN(0),

    // schema types
    VERTEX_LABEL(1),
    EDGE_LABEL(2),
    PROPERTY_KEY(3),
    INDEX_LABEL(4),

    // data types
    VERTEX(101),
    SYS_PROPERTY(102), // system meta
    VERTEX_PROPERTY(103), // vertex property
    EDGE(120),
    EDGE_OUT(120), // edge's direction is OUT for the specified vertex
    EDGE_IN(121), // edge's direction is IN for the specified vertex
    PROPERTY(130), // edge property

    MAX_TYPE(255);

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
