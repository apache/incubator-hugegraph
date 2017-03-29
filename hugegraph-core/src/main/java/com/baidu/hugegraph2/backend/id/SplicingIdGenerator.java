package com.baidu.hugegraph2.backend.id;

import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.structure.HugeEdge;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.util.HashUtil;

public class SplicingIdGenerator extends IdGenerator {

    public static final String ID_SPLITOR = "\u0001";
    public static final String NAME_SPLITOR = "\u0002";

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

    // generate a string id of HugeEdge from:
    //  { source-vertex-id + edge-label + edge-name + target-vertex-id }
    // NOTE: if we use `entry.type()` which is IN or OUT as a part of id,
    // an edge's id will be different due to different directions (belongs to 2 vertex)
    public static Id generate(HugeEdge entry) {
        String id = String.format("%s%s%s%s%s%s%s",
                entry.sourceVertex().id().asString(),
                ID_SPLITOR,
                entry.label(),
                ID_SPLITOR,
                entry.name(),
                ID_SPLITOR,
                entry.targetVertex().id().asString());
        return generate(id);
    }
}
