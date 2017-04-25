package com.baidu.hugegraph.backend.id;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.IndexType;

public class SplicingIdGenerator extends IdGenerator {

    public static final String IDS_SPLITOR = "\u0001";
    public static final String ID_SPLITOR = "\u0002";
    public static final String NAME_SPLITOR = "\u0003";

    /****************************** id generate ******************************/

    // generate a string id of SchemaType from Schema name
    @Override
    public Id generate(SchemaElement entry) {
        // String id = String.format("%x%s%s", entry.type().code(), ID_SPLITOR, entry.name());
        return generate(entry.name());
    }

    // generate a string id of HugeVertex from Vertex name
    @Override
    public Id generate(HugeVertex entry) {
        String id = String.format("%s%s%s", entry.label(), ID_SPLITOR, entry.name());

        // hash for row-key which will be evenly distributed
        // we can also use LongEncoding.encode() to encode the int/long hash if needed
        // id = String.format("%s%s%s", HashUtil.hash(id), ID_SPLITOR, id);

        // TODO: use binary Id with binary fields instead of string id
        return generate(id);
    }

    // generate a string id of HugeEdge from:
    //  { source-vertex-id + edge-label + edge-name + target-vertex-id }
    // NOTE: if we use `entry.type()` which is IN or OUT as a part of id,
    // an edge's id will be different due to different directions (belongs to 2 vertex)
    @Override
    public Id generate(HugeEdge entry) {
        String id = String.format("%s%s%s%s%s%s%s",
                entry.sourceVertex().id().asString(),
                IDS_SPLITOR,
                entry.label(),
                IDS_SPLITOR,
                entry.name(),
                IDS_SPLITOR,
                entry.targetVertex().id().asString());
        return generate(id);
    }

    @Override
    public Id generate(HugeIndex index) {
        if (index.getIndexType() == IndexType.SECONDARY) {
            return generate(index.getPropertyValues());
        } else {
            return generate(index.getIndexLabelId());
        }
    }

    @Override
    public Id[] split(Id id) {
        String[] parts = id.asString().split(IDS_SPLITOR);
        Id[] ids = new Id[parts.length];
        for (int i = 0; i < parts.length; i++) {
            ids[i] = generate(parts[i]);
        }
        return ids;
    }
}
