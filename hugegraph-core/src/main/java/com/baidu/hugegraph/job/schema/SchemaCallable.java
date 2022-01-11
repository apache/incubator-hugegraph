package com.baidu.hugegraph.job.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.job.SysJob;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class SchemaCallable extends SysJob<Object> {

    public static final String REMOVE_SCHEMA = "remove_schema";
    public static final String REBUILD_INDEX = "rebuild_index";
    public static final String CREATE_INDEX = "create_index";
    public static final String CREATE_OLAP = "create_olap";
    public static final String CLEAR_OLAP = "clear_olap";
    public static final String REMOVE_OLAP = "remove_olap";

    protected static final Logger LOG = Log.logger(SchemaCallable.class);

    private static final String SPLITOR = ":";

    protected HugeType schemaType() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[0] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);

        return HugeType.valueOf(parts[0]);
    }

    protected Id schemaId() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[1] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return IdGenerator.of(Long.valueOf(parts[1]));
    }

    protected String schemaName() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[2] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return parts[2];
    }

    public static String formatTaskName(HugeType type, Id id, String name) {
        E.checkNotNull(type, "schema type");
        E.checkNotNull(id, "schema id");
        E.checkNotNull(name, "schema name");
        return String.join(SPLITOR, type.toString(), id.asString(), name);
    }

    protected static void removeIndexLabelFromBaseLabel(SchemaTransaction tx,
                                                        IndexLabel label) {
        HugeType baseType = label.baseType();
        Id baseValue = label.baseValue();
        SchemaLabel schemaLabel;
        if (baseType == HugeType.VERTEX_LABEL) {
            if (VertexLabel.OLAP_VL.id().equals(baseValue)) {
                return;
            }
            schemaLabel = tx.getVertexLabel(baseValue);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            schemaLabel = tx.getEdgeLabel(baseValue);
        }
        assert schemaLabel != null;
        schemaLabel.removeIndexLabel(label.id());
        updateSchema(tx, schemaLabel);
    }

    /**
     * Use reflection to call SchemaTransaction.removeSchema(),
     * which is protected
     * @param tx        The remove operation actual executer
     * @param schema    the schema to be removed
     */
    protected static void removeSchema(SchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = SchemaTransaction.class
                            .getDeclaredMethod("removeSchema",
                                               SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                      "Can't call SchemaTransaction.removeSchema()", e);
        }

    }

    /**
     * Use reflection to call SchemaTransaction.updateSchema(),
     * which is protected
     * @param tx        The update operation actual executer
     * @param schema    the schema to be update
     */
    protected static void updateSchema(SchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = SchemaTransaction.class
                            .getDeclaredMethod("updateSchema",
                                               SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                      "Can't call SchemaTransaction.updateSchema()", e);
        }
    }
}
