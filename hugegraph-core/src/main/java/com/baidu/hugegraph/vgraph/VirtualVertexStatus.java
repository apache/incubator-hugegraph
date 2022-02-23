package com.baidu.hugegraph.vgraph;

public enum VirtualVertexStatus {
    None(0x00, "none"),
    Id(0x01, "id"),
    Property(0x02, "property"),
    OutEdge(0x04, "out_edge"),
    InEdge(0x08, "in_edge"),
    AllEdge(OutEdge.code | InEdge.code, "all_edge"),
    OK((Id.code | Property.code | OutEdge.code | InEdge.code), "ok");

    private byte code;
    private String name;

    VirtualVertexStatus(int code, String name) {
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

    public boolean match(VirtualVertexStatus other) {
        return (this.code & other.code) == this.code;
    }

    public boolean match(byte other) {
        return (this.code & other) == this.code;
    }

    public VirtualVertexStatus or(VirtualVertexStatus other) {
        return fromCode(or(other.code));
    }

    public byte or(byte other) {
        return ((byte) (this.code | other));
    }

    public static VirtualVertexStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return None;
            case 0x01:
                return Id;
            case 0x02:
                return Property;
            case 0x04:
                return OutEdge;
            case 0x08:
                return InEdge;
            case 0x0c:
                return AllEdge;
            case 0x0f:
                return OK;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
