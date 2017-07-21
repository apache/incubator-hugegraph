package com.baidu.hugegraph.backend.id;

import java.util.List;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.StringUtil;

public class SplicingIdGenerator extends IdGenerator {

    /*
     * The following defines can't be java regex special characters:
     * "\^$.|?*+()[{"
     * See: http://www.regular-expressions.info/characters.html
     */
    private static final char ESCAPE = '`';
    private static final char IDS_SPLITOR = '>';
    private static final char ID_SPLITOR = ':';
    private static final char NAME_SPLITOR = '!';

    private static final String ESCAPE_STR = String.valueOf(ESCAPE);
    private static final String IDS_SPLITOR_STR = String.valueOf(IDS_SPLITOR);
    private static final String ID_SPLITOR_STR = String.valueOf(ID_SPLITOR);

    /****************************** id generate ******************************/

    /**
     * Generate a string id of SchemaType from Schema name
     */
    @Override
    public Id generate(SchemaElement entry) {
        /*
         * String id = String.format("%x%s%s", entry.type().code(),
         *                           ID_SPLITOR, escapeId(entry.name()));
         */
        return generate(entry.name());
    }

    /**
     * Generate a string id of HugeEdge from:
     * { source-vertex-id + edge-label + edge-name + target-vertex-id }
     * NOTE: if we use `entry.type()` which is IN or OUT as a part of id,
     * an edge's id will be different due to different directions (belongs
     * to 2 vertex)
     */
    @Override
    public Id generate(HugeEdge edge) {
        return concat(edge.sourceVertex().id().asString(),
                      edge.label(),
                      edge.name(),
                      edge.targetVertex().id().asString());
    }

    /**
     * Generate a string id of HugeVertex from Vertex name
     */
    @Override
    public Id generate(HugeVertex vertex) {
        /*
         * Hash for row-key which will be evenly distributed.
         * We can also use LongEncoding.encode() to encode the int/long hash
         * if needed.
         * id = String.format("%s%s%s", HashUtil.hash(id), ID_SPLITOR, id);
         */
        // TODO: use binary Id with binary fields instead of string id
        return splicing(vertex.label(), vertex.name());
    }

    public static Id concat(String... ids) {
        // NOTE: must support string id when using this method
        String escaped = StringUtil.escape(IDS_SPLITOR, ESCAPE, ids);
        return IdGeneratorFactory.generator().generate(escaped);
    }

    public static String[] split(Id id) {
        return StringUtil.unescape(id.asString(), IDS_SPLITOR_STR, ESCAPE_STR);
    }

    /**
     * Concat property values with name splitor
     */
    public static String concatValues(List<?> values) {
        // Convert the object list to string array
        String[] parts = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            parts[i] = values.get(i).toString();
        }
        return StringUtil.escape(NAME_SPLITOR, ESCAPE, parts);
    }

    public static Id splicing(String... parts) {
        String escaped = StringUtil.escape(ID_SPLITOR, ESCAPE, parts);
        return IdGeneratorFactory.generator().generate(escaped);
    }

    public static String[] parse(Id id) {
        return StringUtil.unescape(id.asString(), ID_SPLITOR_STR, ESCAPE_STR);
    }
}
