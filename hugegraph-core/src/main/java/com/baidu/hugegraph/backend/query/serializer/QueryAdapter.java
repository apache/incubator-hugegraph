package com.baidu.hugegraph.backend.query.serializer;

import com.baidu.hugegraph.backend.query.Condition;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class QueryAdapter extends AbstractSerializerAdapter<Condition> {

    static HashMap<String,Type> CLS = null;
    static {
        CLS = new HashMap(){{
            put("A", Condition.And.class);
            put("O", Condition.Or.class);
            put("S", Condition.SyspropRelation.class);
            put("U", Condition.UserpropRelation.class);
        }};
    }

    @Override
    public Map<String, Type> validType() {
        return CLS;
    }
}

