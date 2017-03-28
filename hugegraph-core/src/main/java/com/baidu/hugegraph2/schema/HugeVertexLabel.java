package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.IndexType;
import com.baidu.hugegraph2.type.schema.VertexLabel;
import com.baidu.hugegraph2.util.StringUtil;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel extends VertexLabel {

    private static final Logger logger = LoggerFactory.getLogger(HugeVertexLabel.class);

    private Set<String> primaryKeys;

    public HugeVertexLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
        this.primaryKeys = null;
    }

    public Set<String> primaryKeys() {
        return primaryKeys;
    }

    @Override
    public VertexLabel primaryKeys(String... keys) {
        // Check whether the properties contains the specified keys
        Preconditions.checkNotNull(properties);
        for (String key : keys) {
            Preconditions
                    .checkArgument(properties.containsKey(key),
                            "Properties must contain the specified key : " + key);
        }
        if (this.primaryKeys == null) {
            this.primaryKeys = new HashSet<>();
        }
        this.primaryKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public String toString() {
        return String.format("{name=%s}", this.name);
    }

    public String schema() {
        schema = "schema.vertexLabel(\"" + name + "\")"
                + "." + propertiesSchema()
                + StringUtil.descSchema("primaryKeys", primaryKeys)
                + ".create();";
        return schema;
    }

    public void create() {
        try {
            this.transaction.addVertexLabel(this);
            this.transaction.commit();
        } catch (BackendException e) {
            logger.error("Failed to commit schema changes: {}", e.getMessage());
            try {
                this.transaction.rollback();
            } catch (BackendException e2) {
                // TODO: any better ways?
                logger.error("Failed to rollback schema changes: {}", e2.getMessage());
            }
        }
    }

    public void remove() {
        this.transaction.removeVertexLabel(this.name);
    }
}
