package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com
 */
public final class HgPrivate {

    private static HgPrivate instance = new HgPrivate();

    private HgPrivate() {
    }

    static HgPrivate of() {
        return instance;
    }

}
