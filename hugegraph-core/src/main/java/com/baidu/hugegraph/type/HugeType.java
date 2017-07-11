package com.baidu.hugegraph.type;

public enum HugeType {

    UNKNOWN(0),

    /* Schema types */
    VERTEX_LABEL(1),
    EDGE_LABEL(2),
    PROPERTY_KEY(3),
    INDEX_LABEL(4),

    /* Data types */
    VERTEX(101),
    // System meta
    SYS_PROPERTY(102),
    // Property
    PROPERTY(103),
    EDGE(120),
    // Edge's direction is OUT for the specified vertex
    EDGE_OUT(120),
    // Edge's direction is IN for the specified vertex
    EDGE_IN(121),

    SECONDARY_INDEX(150),
    SEARCH_INDEX(151),

    MAX_TYPE(255);

    // HugeType define
    private byte type = 0;

    private HugeType(int type) {
        assert type < 256;
        this.type = (byte) type;
    }

    public byte code() {
        return this.type;
    }
}
