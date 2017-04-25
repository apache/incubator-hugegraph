package com.baidu.hugegraph.schema;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/27.
 */
public abstract class SchemaElement implements Namifiable, HugeType {

    private static final Logger logger = LoggerFactory.getLogger(SchemaElement.class);

    protected String name;

    // TODO:This is a questionable place，mutual reference
    protected SchemaTransaction transaction;
    protected Map<String, PropertyKey> properties;
    protected Set<String> indexNames;

    public SchemaElement(String name, SchemaTransaction transaction) {
        this.name = name;
        this.transaction = transaction;
        this.properties = new HashMap<>();
        this.indexNames = new LinkedHashSet<>();
    }

    public Map<String, PropertyKey> properties() {
        return this.properties;
    }

    public SchemaElement properties(String... propertyNames) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        for (String propertyName : propertyNames) {
            this.properties.put(propertyName, new HugePropertyKey(propertyName, this.transaction));
        }
        return this;
    }

    protected String propertiesSchema() {
        String props = "";
        if (this.properties != null) {
            for (String propertyName : this.properties.keySet()) {
                props += "\"";
                props += propertyName;
                props += "\",";
            }
        }
        int endIdx = props.lastIndexOf(",") > 0 ? props.length() - 1 : props.length();
        return "properties(" + props.substring(0, endIdx) + ")";
    }

    public Set<String> indexNames() {
        return this.indexNames;
    }

    public abstract SchemaElement indexNames(String... names);

    protected boolean commit() {
        try {
            this.transaction.commit();
            return true;
        } catch (BackendException e) {
            logger.error("Failed to commit schema changes: {}", e.getMessage());
            try {
                this.transaction.rollback();
            } catch (BackendException e2) {
                logger.error("Failed to rollback schema changes: {}", e2.getMessage());
            }
        }
        return false;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return schema();
    }

    public static boolean isSchema(HugeTypes type) {
        if (type == HugeTypes.VERTEX_LABEL
                || type == HugeTypes.EDGE_LABEL
                || type == HugeTypes.PROPERTY_KEY
                /* || type == HugeTypes.INDEX_LABEL */) {
            return true;
        }
        return false;
    }

    public abstract String schema();

    public abstract void create();

    public abstract void remove();
}
