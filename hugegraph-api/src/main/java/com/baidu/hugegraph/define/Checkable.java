package com.baidu.hugegraph.define;

public interface Checkable {

    public void checkCreate(boolean isBatch);

    public default void checkUpdate() {
        this.checkCreate(false);
    }
}
