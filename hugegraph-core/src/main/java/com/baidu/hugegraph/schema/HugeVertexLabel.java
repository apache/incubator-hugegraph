package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.StringUtil;
import com.google.common.base.Preconditions;

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

    @Override
    public String schema() {
        return "schema.vertexLabel(\"" + this.name + "\")"
                + "." + propertiesSchema()
                + StringUtil.descSchema("primaryKeys", this.primaryKeys)
                + ".create();";
    }

    @Override
    public VertexLabel create() {

        StringUtil.checkName(this.name);
        // Try to read
        VertexLabel vertexLabel = this.transaction().getVertexLabel(this.name);
        // if vertexLabel exist and checkExits
        if (vertexLabel != null && this.checkExits) {
            throw new HugeException("The vertexlabel:" + this.name + " has exised.");
        }

        this.checkPrimaryKeys();
        this.transaction().addVertexLabel(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removeVertexLabel(this.name);
    }

    private void checkPrimaryKeys() {
        if (this.primaryKeys != null && !this.primaryKeys.isEmpty()) {
            // Check whether the properties contains the specified keys
            Preconditions.checkNotNull(this.properties, "properties can not be null");
            Preconditions.checkArgument(!this.properties.isEmpty(), "properties can not be empty");
            for (String key : this.primaryKeys) {
                Preconditions.checkArgument(this.properties.containsKey(key),
                        "properties must contain the specified key : " + key);
            }
        }
    }
}
