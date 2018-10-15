package com.baidu.hugegraph.job.schema;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.job.Job;
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
}