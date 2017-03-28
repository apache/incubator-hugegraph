package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
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
        this.primaryKeys = new LinkedHashSet<>();
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
        if (this.transaction.getVertexLabel(this.name) != null) {
            throw new HugeException("The vertexlabel:" + this.name + " has exised.");
        }
        this.transaction.addVertexLabel(this);
        this.commit();
    }

    public void remove() {
        this.transaction.removeVertexLabel(this.name);
    }
}
