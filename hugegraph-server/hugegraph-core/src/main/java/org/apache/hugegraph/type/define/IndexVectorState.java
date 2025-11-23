package org.apache.hugegraph.type.define;

public enum IndexVectorState implements SerialEnum{

    // after written to vector map
    BUILDING(1, "building"),

    // after flushed to disk
    DISK_FLUSHED(2, "disk_flushed"),

    // after marked deleted
    DELETING(3, "deleting"),

    DIRTY_PREFIX(4, "dirty_prefix");

    private byte code = 0;
    private String name = null;

    IndexVectorState(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    static {
        SerialEnum.register(IndexVectorState.class);
    }

    @Override
    public byte code(){ return code; }

    public String string() {
        return this.name;
    }

    public boolean isBuilding() {
        return this == BUILDING;
    }

    public boolean isDiskFlushed() {
        return this == DISK_FLUSHED;
    }

    public boolean isDeleting() {
        return this == DELETING;
    }

    public boolean isDirty() {
        return this == DIRTY_PREFIX;
    }
}
