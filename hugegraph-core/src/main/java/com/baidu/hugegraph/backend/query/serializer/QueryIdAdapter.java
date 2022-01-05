package com.baidu.hugegraph.backend.query.serializer;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.sun.corba.se.spi.ior.ObjectId;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class QueryIdAdapter extends AbstractSerializerAdapter<Id>  {

    static HashMap<String,Type> CLS = null;
    static {
        CLS = new HashMap(){{
            put("E", EdgeId.class);
            put("S", IdGenerator.StringId.class);
            put("L", IdGenerator.LongId.class);
            put("U", IdGenerator.UuidId.class);
            put("O", ObjectId.class);
            put("B", BinaryBackendEntry.BinaryId.class);
        }};
    }

    @Override
    public Map<String, Type> validType() {
        return CLS;
    }
}