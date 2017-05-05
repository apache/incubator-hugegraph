package com.baidu.hugegraph.schema;

import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.StringUtil;

/**
 * Created by liningrui on 2017/4/21.
 */
public class HugeIndexLabel extends IndexLabel {

    private HugeTypes baseType;
    private String baseValue;
    private IndexType indexType;
    private Set<String> indexFields;

    public HugeIndexLabel(String name) {
        this(name, null, null, null);
    }

    public HugeIndexLabel(String name, HugeTypes baseType,
            String baseValue, SchemaTransaction transaction) {
        super(name, transaction);
        this.baseType = baseType;
        this.baseValue = baseValue;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new HashSet<>();
    }

    @Override
    public HugeIndexLabel indexNames(String... names) {
        throw new HugeException("can not build index for index label object.");
    }

    @Override
    public HugeTypes baseType() {
        return this.baseType;
    }

    @Override
    public String baseValue() {
        return this.baseValue;
    }

    @Override
    public IndexType indexType() {
        return this.indexType;
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    @Override
    public Set<String> indexFields() {
        return this.indexFields;
    }

    @Override
    public IndexLabel by(String... indexFields) {
        for (String indexFiled : indexFields) {
            this.indexFields.add(indexFiled);
        }
        return this;
    }

    @Override
    public IndexLabel secondary() {
        this.indexType = IndexType.SECONDARY;
        return this;
    }

    @Override
    public IndexLabel search() {
        this.indexType = IndexType.SEARCH;
        return this;
    }

    @Override
    public String schema() {
        String schema = "";
        schema = ".index(\"" + this.name + "\")"
                + StringUtil.descSchema("by", this.indexFields)
                + "." + this.indexType.string() + "()";
        return schema;
    }

    @Override
    public void create() {
        if (this.transaction.getIndexLabel(this.name) != null) {
            throw new HugeException("The indexLabel:" + this.name + " has exised.");
        }

        // TODO: should wrap update and add operation in one transaction.
        this.updateSchemaIndexName(this.baseType, this.baseValue, this.name);

        // TODO: need to check param.
        this.transaction.addIndexLabel(this);
    }

    protected void updateSchemaIndexName(HugeTypes baseType, String baseValue, String indexName) {
        switch (baseType) {
            case VERTEX_LABEL:
                VertexLabel vertexLabel = this.transaction.getVertexLabel(baseValue);
                vertexLabel.indexNames(indexName);
                this.transaction.addVertexLabel(vertexLabel);
                break;
            case EDGE_LABEL:
                EdgeLabel edgeLabel = this.transaction.getEdgeLabel(baseValue);
                edgeLabel.indexNames(indexName);
                this.transaction.addEdgeLabel(edgeLabel);
                break;
            case PROPERTY_KEY:
                PropertyKey propertyKey = this.transaction.getPropertyKey(baseValue);
                propertyKey.indexNames(indexName);
                this.transaction.addPropertyKey(propertyKey);
                break;
            default:
                String msg = String.format("can not update index name of schema type: %s",
                        baseType.toString());
                throw new HugeException(msg);
        }
    }

    @Override
    public void remove() {
        this.transaction.removeIndexLabel(this.name);
    }
}
