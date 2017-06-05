package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel extends VertexLabel {

    private Set<String> primaryKeys;

    public HugeVertexLabel(String name) {
        super(name);
        this.primaryKeys = new LinkedHashSet<>();
    }

    @Override
    public Set<String> primaryKeys() {
        return this.primaryKeys;
    }

    @Override
    public VertexLabel primaryKeys(String... keys) {
        this.primaryKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public HugeVertexLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    public void checkExists(boolean checkExists) {
        this.checkExits = checkExists;
    }

    @Override
    public String schema() {
        return "schema.vertexLabel(\"" + this.name + "\")"
                + "." + propertiesSchema()
                + StringUtil.desc("primaryKeys", this.primaryKeys)
                + ".create();";
    }

    @Override
    public VertexLabel create() {

        StringUtil.checkName(this.name);
        // Try to read
        VertexLabel vertexLabel = this.transaction().getVertexLabel(this.name);
        // if vertexLabel exist and checkExits
        if (vertexLabel != null && this.checkExits) {
            throw new HugeException(String.format(
                    "The vertex label '%s' has exised.", this.name));
        }

        this.checkProperties();
        this.checkPrimaryKeys();

        this.transaction().addVertexLabel(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removeVertexLabel(this.name);
    }

    @Override
    protected HugeVertexLabel copy() throws CloneNotSupportedException {
        HugeVertexLabel vertexLabel = new HugeVertexLabel(this.name);
        vertexLabel.properties = new LinkedHashSet<>();
        for (String property : this.properties) {
            vertexLabel.properties.add(property);
        }
        vertexLabel.primaryKeys = new LinkedHashSet<>();
        for (String primaryKey : this.primaryKeys) {
            vertexLabel.primaryKeys.add(primaryKey);
        }
        vertexLabel.indexNames = new LinkedHashSet<>();
        for (String indexName : this.indexNames) {
            vertexLabel.indexNames.add(indexName);
        }
        return vertexLabel;
    }

    private void checkProperties() {
        E.checkNotNull(this.properties, "The properties of '%s'", this.name);
        E.checkNotEmpty(this.properties, "The properties of '%s'", this.name);
        // If properties is not empty, check all property.
        for (String pk : this.properties) {
            PropertyKey propertyKey = this.transaction().getPropertyKey(pk);
            E.checkArgumentNotNull(propertyKey,
                    "Undefined property key '%s' in the vertex label '%s'",
                    pk, this.name);
        }
    }

    private void checkPrimaryKeys() {
        E.checkNotNull(this.primaryKeys, HugeKeys.PRIMARY_KEYS.string());
        E.checkNotEmpty(this.primaryKeys, HugeKeys.PRIMARY_KEYS.string());

        // Use loop instead containAll for more detailed exception info.
        for (String key : this.primaryKeys) {
            E.checkArgument(this.properties.contains(key),
                    "The primary key '%s' of vertex label '%s' must be "
                    + "contained in %s", key, this.name, this.properties);
        }
    }
}
