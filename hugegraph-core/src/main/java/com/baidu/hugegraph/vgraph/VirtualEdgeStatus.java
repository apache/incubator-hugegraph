package com.baidu.hugegraph.vgraph;

public enum VirtualEdgeStatus {
    None(0x00, "none"),
    Id(0x01, "id"),
    Property(0x02, "property"),
    OK((Id.code | Property.code), "ok");

    private byte code;
    private String name;

    VirtualEdgeStatus(int code, String name) {
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

    public boolean match(VirtualEdgeStatus other) {
        return (this.code & other.code) == this.code;
    }

    public boolean match(byte other) {
        return (this.code & other) == this.code;
    }

    public VirtualEdgeStatus or(VirtualEdgeStatus other) {
        return fromCode(or(other.code));
    }

    public byte or(byte other) {
        return ((byte) (this.code | other));
    }

    public static VirtualEdgeStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return None;
            case 0x01:
                return Id;
            case 0x02:
                return Property;
            case 0x03:
                return OK;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
