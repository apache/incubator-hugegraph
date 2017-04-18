package com.baidu.hugegraph.backend.query;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by jishilei on 2017/3/28.
 */
public class HugeQuery implements Query {

    private Map<String, Object> conditions;
    private int limit;

    public HugeQuery() {
        conditions = new LinkedHashMap<String, Object>();
        limit = Integer.MAX_VALUE;

    }

    public Map<String, Object> conditions() {
        return conditions;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public HugeQuery has(String key, Object value) {
        this.conditions.put(key, value);
        return this;
    }

    @Override
    public HugeQuery limit(int max) {
        this.limit = max;
        return this;
    }

}
