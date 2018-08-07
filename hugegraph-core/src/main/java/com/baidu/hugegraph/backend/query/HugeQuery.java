package com.baidu.hugegraph.backend.query;

import java.util.LinkedHashMap;
import java.util.Map;

import com.baidu.hugegraph.type.HugeTypes;

/**
 * Created by jishilei on 2017/3/28.
 */
public class HugeQuery implements Query {

    private HugeTypes resultType;
    private Map<String, Object> conditions;
    private int limit;

    public HugeQuery(HugeTypes resultType) {
        this.resultType = resultType;
        this.conditions = new LinkedHashMap<String, Object>();
        this.limit = Integer.MAX_VALUE;

    }

    @Override
    public Map<String, Object> conditions() {
        return this.conditions;
    }

    @Override
    public int limit() {
        return this.limit;
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

    @Override
    public HugeTypes resultType() {
        return this.resultType;
    }

}
