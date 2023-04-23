package com.baidu.hugegraph.store.client;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/26
 */
public class HgPrivate {
    private final static HgPrivate instance = new HgPrivate();

    static HgPrivate getInstance() {
        return instance;
    }

    private HgPrivate() {

    }

}
