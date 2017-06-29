package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.util.E;

/**
 * Created by liningrui on 2017/3/27.
 */
public abstract class SchemaElement implements Namifiable, Typifiable {

    protected String name;
    protected boolean checkExist;
    protected Set<String> properties;
    protected Set<String> indexNames;

    private SchemaTransaction transaction;

    public SchemaElement(String name) {
        this.name = name;
        this.checkExist = true;
        this.properties = new LinkedHashSet<>();
        this.indexNames = new LinkedHashSet<>();
        this.transaction = null;
    }

    protected void transaction(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    protected SchemaTransaction transaction() {
        E.checkNotNull(this.transaction, "schema transaction");
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
        StringBuilder sb = new StringBuilder();
        for (String propertyName : this.properties) {
            sb.append("\"").append(propertyName).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".properties(%s)", sb.substring(0, endIdx));
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
