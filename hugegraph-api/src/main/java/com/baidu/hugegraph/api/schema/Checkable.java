package com.baidu.hugegraph.api.schema;

public interface Checkable {

    public void checkCreate(boolean isBatch);

    public default void checkUpdate() {
        this.checkCreate(false);
    }
}
