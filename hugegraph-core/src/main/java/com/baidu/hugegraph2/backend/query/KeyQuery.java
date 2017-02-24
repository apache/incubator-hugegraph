package com.baidu.hugegraph2.backend.query;

import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/19.
 */
public class KeyQuery implements Query {

    private Object key;

    public KeyQuery(Object key) {
        Preconditions.checkNotNull(key);
    }

    @Override
    public boolean hasLimit() {
        return false;
    }

    @Override
    public int getLimit() {
        return 1;
    }
}
