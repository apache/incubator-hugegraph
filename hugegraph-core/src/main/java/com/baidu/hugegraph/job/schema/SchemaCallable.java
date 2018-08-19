package com.baidu.hugegraph.job.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public abstract class SchemaCallable extends Job<Object> {

    public static final String REMOVE_SCHEMA = "remove_schema";
    public static final String REBUILD_INDEX = "rebuild_index";

    private static final String SPLITOR = ":";

    protected HugeType schemaType() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR);
        E.checkState(parts.length == 2 && parts[0] != null,
                     "Task name should be formatted to String 'TYPE:ID', " +
                     "but got '%s'", name);

        return HugeType.valueOf(parts[0]);
    }

    protected Id schemaId() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR);
        E.checkState(parts.length == 2 && parts[1] != null,
                     "Task name should be formatted to String 'TYPE:ID', " +
                     "but got '%s'", name);
        return IdGenerator.of(Long.valueOf(parts[1]));
    }

    public static String formatTaskName(HugeType schemaType, Id schemaId) {
        E.checkNotNull(schemaType, "schema type");
        E.checkNotNull(schemaId, "schema id");
        return String.join(SPLITOR, schemaType.toString(), schemaId.toString());
    }

    protected static void removeIndexLabelFromBaseLabel(SchemaTransaction tx,
                                                        IndexLabel label) {
        HugeType baseType = label.baseType();
        Id baseValue = label.baseValue();
        SchemaLabel schemaLabel;
        if (baseType == HugeType.VERTEX_LABEL) {
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
