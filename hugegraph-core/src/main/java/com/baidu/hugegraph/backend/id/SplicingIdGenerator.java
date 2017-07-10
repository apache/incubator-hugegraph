package com.baidu.hugegraph.backend.id;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;

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
        String id = String.format("%s%s%s",
                                  entry.label(),
                                  ID_SPLITOR,
                                  entry.name());

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

    public static Id concat(String... ids) {
        // NOTE: must support string id when using this method
        String id = String.join(IDS_SPLITOR, ids);
        return IdGeneratorFactory.generator().generate(id);
    }

    public static String[] split(Id id) {
        String[] ids = id.asString().split(IDS_SPLITOR);
        return ids;
    }

    public static String concatValues(Iterable<?> values) {
        // TODO: use a better delimiter
        return StringUtils.join(values, NAME_SPLITOR);
    }

    public static String[] splitValues(String values) {
        return values.split(IDS_SPLITOR);
    }

    public static Id splicing(String... parts) {
        String id = String.join(ID_SPLITOR, parts);
        return IdGeneratorFactory.generator().generate(id);
    }

    public static String[] parse(Id id) {
        String[] parts = id.asString().split(ID_SPLITOR);
        return parts;
    }
}
