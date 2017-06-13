package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/3/27.
 */
public abstract class SchemaElement implements Namifiable, Typifiable {

    protected String name;
    protected boolean checkExits;
    protected Set<String> properties;
    protected Set<String> indexNames;

    // TODO: Don't reference SchemaTransaction here(to avoid mutual reference)
    private SchemaTransaction transaction;

    public SchemaElement(String name) {
        this.name = name;
        this.checkExits = true;
        this.properties = new LinkedHashSet<>();
        this.indexNames = new LinkedHashSet<>();
        this.transaction = null;
    }

    protected void transaction(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    protected SchemaTransaction transaction() {
        Preconditions.checkNotNull(this.transaction,
                "Transaction must not be null when creating");
        return this.transaction;
    }

    public Set<String> properties() {
        return this.properties;
    }

    public SchemaElement properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    protected String propertiesSchema() {
        String props = "";
        for (String propertyName : this.properties) {
            props += "\"";
            props += propertyName;
            props += "\",";
        }
        int endIdx = props.lastIndexOf(",") > 0 ? props.length() - 1 : props.length();
        return "properties(" + props.substring(0, endIdx) + ")";
    }

    public Set<String> indexNames() {
        return this.indexNames;
    }

    public abstract SchemaElement indexNames(String... names);

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return schema();
    }

    public static boolean isSchema(HugeType type) {
        if (type == HugeType.VERTEX_LABEL
                || type == HugeType.EDGE_LABEL
                || type == HugeType.PROPERTY_KEY
                || type == HugeType.INDEX_LABEL) {
            return true;
        }
        return false;
    }

    public abstract String schema();

    public abstract SchemaElement ifNotExist();

    public abstract SchemaElement create();

    public abstract SchemaElement append();

    public abstract void remove();

    protected abstract SchemaElement copy() throws CloneNotSupportedException;
}
