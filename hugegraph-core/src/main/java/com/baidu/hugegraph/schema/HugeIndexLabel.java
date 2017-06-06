package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringUtil;

/**
 * Created by liningrui on 2017/4/21.
 */
public class HugeIndexLabel extends IndexLabel {

    private SchemaElement element;

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
        this.element = element;
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

    public void baseType(HugeType baseType) {
        this.baseType = baseType;
    }

    @Override
    public String baseValue() {
        return this.baseValue;
    }

    public void baseValue(String baseValue) {
        this.baseValue = baseValue;
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

    public void indexFields(String... indexFields) {
        this.indexFields.addAll(Arrays.asList(indexFields));
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

    public void checkExists(boolean checkExists) {
        this.checkExits = checkExists;
    }

    @Override
    public String schema() {
        String schema = "";
        schema = ".index(\"" + this.name + "\")"
                + StringUtil.desc("by", this.indexFields)
                + "." + this.indexType.string() + "()";
        return schema;
    }

    @Override
    public IndexLabel create() {

        StringUtil.checkName(this.name);
        IndexLabel indexLabel = this.transaction().getIndexLabel(this.name);
        if (indexLabel != null && checkExits) {
            throw new HugeException(String.format(
                    "The index label '%s' has exised.", this.name));
        }

        // check field
        this.checkFields();

        SchemaElement cloneElement = null;
        try {
            cloneElement = element.copy();
        } catch (CloneNotSupportedException e) {
            throw new HugeException(String.format("It's not allowed to make "
                    + "index on this schema type: '%s'.", this.element.type()));
        }

        // TODO: should wrap update and add operation in one transaction.
        this.updateSchemaIndexName(cloneElement);

        this.transaction().addIndexLabel(this);

        // If addIndexLabel successed, update schema element indexNames
        this.element.indexNames(this.name);
        return this;
    }

    private void checkFields() {
        // search index must build on single numeric column
        if (this.indexType == IndexType.SEARCH) {
            E.checkArgument(this.indexFields.size() == 1,
                    "Search index can only build on one property, " +
                    "but got %d properties: '%s'", this.indexFields.size(),
                    this.indexFields);
            String field = this.indexFields.iterator().next();
            PropertyKey propertyKey = this.transaction().getPropertyKey(field);
            E.checkArgument(NumericUtil.isNumber(propertyKey.dataType().clazz()),
                    "Search index can only build on numeric property, " +
                    "but got %s(%s)", propertyKey.dataType(), propertyKey.name());
        }
    }

    protected void updateSchemaIndexName(SchemaElement cloneElement) {
        cloneElement.indexNames(this.name);
        switch (this.baseType) {
            case VERTEX_LABEL:
                this.transaction().addVertexLabel((VertexLabel) cloneElement);
                break;
            case EDGE_LABEL:
                this.transaction().addEdgeLabel((EdgeLabel) cloneElement);
                break;
            case PROPERTY_KEY:
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

    @Override
    protected SchemaElement copy() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("IndexLabel object can't "
                + "support copy.");
    }
}
