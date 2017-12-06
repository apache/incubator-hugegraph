package com.baidu.hugegraph.type.define;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.type.HugeType;

public enum Directions implements SerialEnum {

    OUT(0, "out"),
    IN(1, "in"),
    BOTH(2, "both");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(Directions.class);
    }

    Directions(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public HugeType type() {
        switch (this) {
            case OUT:
                return HugeType.EDGE_OUT;
            case IN:
                return HugeType.EDGE_IN;
            default:
                throw new IllegalArgumentException(String.format(
                          "Can't convert direction '%s' to HugeType", this));
        }
    }

    public static Directions convert(Direction direction) {
        switch (direction) {
            case OUT:
                return OUT;
            case IN:
                return IN;
            case BOTH:
                return BOTH;
            default:
                throw new AssertionError(String.format(
                          "Unrecognized direction: '%s'", direction));
        }
    }

    public static Directions convert(HugeType edgeType) {
        switch (edgeType) {
            case EDGE_OUT:
                return OUT;
            case EDGE_IN:
                return IN;
            default:
                throw new IllegalArgumentException(String.format(
                          "Can't convert type '%s' to Direction", edgeType));
        }
    }
}
