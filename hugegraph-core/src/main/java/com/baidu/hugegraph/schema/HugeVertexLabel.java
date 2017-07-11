package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;

public class HugeVertexLabel extends VertexLabel {

    private List<String> primaryKeys;

    public HugeVertexLabel(String name) {
        super(name);
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public List<String> primaryKeys() {
        return this.primaryKeys;
    }

    @Override
    public VertexLabel primaryKeys(String... keys) {
        for (String key : keys) {
            if (!this.primaryKeys.contains(key)) {
                this.primaryKeys.add(key);
            }
        }
        return this;
    }

    @Override
    public HugeVertexLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    public void checkExist(boolean checkExists) {
        this.checkExist = checkExists;
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.makePropertyKey(\"").append(this.name).append("\")");
        sb.append(this.propertiesSchema());
        sb.append(this.primaryKeysSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    private String primaryKeysSchema() {
        return StringUtil.desc("primaryKeys", this.primaryKeys);
    }

    @Override
    public VertexLabel create() {

        StringUtil.checkName(this.name);
        // Try to read
        VertexLabel vertexLabel = this.transaction().getVertexLabel(this.name);
        // if vertexLabel exist and checkExist
        if (vertexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("vertex label", this.name);
            } else {
                return vertexLabel;
            }
        }

        this.checkProperties();
        this.checkPrimaryKeys();

        this.transaction().addVertexLabel(this);
        return this;
    }

    @Override
    public SchemaElement append() {

        StringUtil.checkName(this.name);
        // Don't allow user to modify some stable properties.
        this.checkStableVars();

        this.checkProperties();

        // Try to read
        VertexLabel vertexLabel = this.transaction().getVertexLabel(this.name);
        if (vertexLabel == null) {
            throw new HugeException("Can't append the vertex label '%s' " +
                                    "since it doesn't exist",
                                    this.name);
        }

        vertexLabel.properties().addAll(this.properties);

        this.transaction().addVertexLabel(vertexLabel);
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
        vertexLabel.primaryKeys = new ArrayList<>();
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
        E.checkNotNull(this.properties, "properties", this.name);
        E.checkNotEmpty(this.properties, "properties", this.name);
        // If properties is not empty, check all property.
        for (String pk : this.properties) {
            PropertyKey propertyKey = this.transaction().getPropertyKey(pk);
            E.checkArgumentNotNull(
                    propertyKey,
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
                            "The primary key '%s' of vertex label '%s' must " +
                            "be contained in %s", key, this.name,
                            this.properties);
        }
    }

    private void checkStableVars() {
        // Don't allow to append sort keys.
        if (!this.primaryKeys.isEmpty()) {
            throw new NotAllowException("Not allowed to append primary keys " +
                                        "for existed vertex label '%s'",
                                        this.name);
        }
        if (!this.indexNames.isEmpty()) {
            throw new NotAllowException("Not allowed to append indexes for " +
                                        "existed vertex label '%s'", this.name);
        }
    }
}
