package com.baidu.hugegraph.schema;

import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringUtil;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/4/21.
 */
public class HugeIndexLabel extends IndexLabel {

    private HugeType baseType;
    private String baseValue;
    private IndexType indexType;
    private Set<String> indexFields;

    public HugeIndexLabel(String name) {
        this(name, null, null);
    }

    public HugeIndexLabel(String name, HugeType baseType, String baseValue) {
        super(name);
        this.baseType = baseType;
        this.baseValue = baseValue;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new HashSet<>();
    }

    @Override
    public IndexLabel on(SchemaElement element) {
        this.baseType = element.type();
        this.baseValue = element.name();
        return this;
    }

    @Override
    public HugeIndexLabel indexNames(String... names) {
        throw new HugeException("can not build index for index label object.");
    }

    @Override
    public HugeType baseType() {
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
    public IndexLabel create() {

        StringUtil.verifyName(this.name);
        IndexLabel indexLabel = this.transaction().getIndexLabel(this.name);
        if (indexLabel != null && checkExits) {
            throw new HugeException("The indexLabel:" + this.name + " has exised.");
        }

        // check field
        this.checkFields();

        // TODO: should wrap update and add operation in one transaction.
        this.updateSchemaIndexName(this.baseType, this.baseValue);

        // TODO: need to check param.
        this.transaction().addIndexLabel(this);

        return this;
    }

    private void checkFields() {
        // search index must build on single numeric column
        if (this.indexType == IndexType.SEARCH) {
            Preconditions.checkArgument(this.indexFields.size() == 1,
                    "Search index can only build on one property, "
                            + "but properties: " + this.indexFields + " is multiple.");
            String propertyName = this.indexFields.iterator().next();
            PropertyKey propertyKey = this.transaction().getPropertyKey(propertyName);
            Preconditions.checkArgument(NumericUtil.isNumber(propertyKey.dataType().clazz()),
                    "Search index can only build on numeric property, "
                            + "but property: " + propertyName + " is not.");
        }
    }

    protected void updateSchemaIndexName(HugeType baseType, String baseValue) {
        SchemaManager schema = this.transaction().graph().schema();
        switch (baseType) {
            case VERTEX_LABEL:
                VertexLabel vertexLabel = schema.vertexLabel(baseValue);
                vertexLabel.indexNames(this.name);
                this.transaction().addVertexLabel(vertexLabel);
                break;
            case EDGE_LABEL:
                EdgeLabel edgeLabel = schema.edgeLabel(baseValue);
                edgeLabel.indexNames(this.name);
                this.transaction().addEdgeLabel(edgeLabel);
                break;
            case PROPERTY_KEY:
                PropertyKey propertyKey = schema.propertyKey(baseValue);
                propertyKey.indexNames(this.name);
                this.transaction().addPropertyKey(propertyKey);
                break;
            default:
                throw new HugeException(String.format(
                        "Can not update index name of schema type: %s",
                        baseType));
        }
    }

    @Override
    public void remove() {
        this.transaction().removeIndexLabel(this.name);
    }
}
