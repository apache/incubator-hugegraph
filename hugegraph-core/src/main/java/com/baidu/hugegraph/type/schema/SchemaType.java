package com.baidu.hugegraph.type.schema;

/**
 * Created by jishilei on 2017/3/28.
 */
public enum SchemaType {
    // schema types
    VERTEX_LABEL(1),
    EDGE_LABEL(2),
    PROPERTY_KEY(3),
    VERTEX_INDEX(4),
    EDGE_INDEX(5);

    // SchemaType define
    private byte type = 0;

    private SchemaType(int type) {
        assert type < 256;
        this.type = (byte) type;
    }

    public byte code() {
        return this.type;
    }
}
