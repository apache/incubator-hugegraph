package com.baidu.hugegraph2.backend.query;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jishilei on 17/3/19.
 */
public class SliceQuery implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SliceQuery.class);
    public Map<String, Object> conditions;

    public SliceQuery() {
        conditions = new HashMap<String, Object>();
    }

    public void condition(String k, Object v) {
        logger.info(String.format("add condition %s=%s", k, v));
        this.conditions.put(k, v);
    }

    public Map<String, Object> conditions() {
        return conditions;
    }
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
