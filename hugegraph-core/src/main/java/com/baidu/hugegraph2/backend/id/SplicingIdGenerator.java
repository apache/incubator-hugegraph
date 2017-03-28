package com.baidu.hugegraph2.backend.id;

import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.util.HashUtil;

public class SplicingIdGenerator extends IdGenerator {

    public static final String ID_SPLITOR = "\u0001";

    /****************************** id generate ******************************/

    // generate a string id of SchemaType from Schema type and name
    public static Id generate(SchemaElement entry) {
        String id = String.format("%x%s%s", entry.type().code(), ID_SPLITOR, entry.name());
        return generate(id);
    }

    // generate a string id of HugeVertex from Vertex name
    public static Id generate(HugeVertex entry) {
        String id = String.format("%s%s%s", entry.label(), ID_SPLITOR, entry.name());
        // hash for row-key which will be evenly distributed
        // we can also use LongEncoding.encode() to encode the int/long hash if needed
        id = String.format("%s%s%s", HashUtil.hash(id), ID_SPLITOR, id);
        // TODO: use binary Id with binary fields instead of string id
        return generate(id);
    }
}
