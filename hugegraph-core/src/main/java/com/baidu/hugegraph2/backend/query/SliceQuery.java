package com.baidu.hugegraph2.backend.query;

/**
 * Created by jishilei on 17/3/19.
 */
public class SliceQuery implements Query {

    // start - end

    @Override
    public boolean hasLimit() {
        return false;
    }

    @Override
    public int limit() {
        return 0;
    }
}
