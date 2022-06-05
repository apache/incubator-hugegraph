package com.baidu.hugegraph.define;

public interface Checkable {

    void checkCreate(boolean isBatch);

    default void checkUpdate() {
        this.checkCreate(false);
    }
}
