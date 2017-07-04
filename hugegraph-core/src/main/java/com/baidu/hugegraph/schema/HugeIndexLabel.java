package com.baidu.hugegraph.schema;

import java.util.LinkedList;
import java.util.List;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
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
    private List<String> indexFields;

    public HugeIndexLabel(String name) {
        this(name, null, null);
    }

    public HugeIndexLabel(String name, HugeType baseType, String baseValue) {
        super(name);
        this.baseType = baseType;
        this.baseValue = baseValue;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new LinkedList<>();
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
        throw new HugeException("Can't build index for index label.");
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

    @Override
    public HugeType queryType() {
        switch (this.baseType) {
            case VERTEX_LABEL:
                return HugeType.VERTEX;
            case EDGE_LABEL:
                return HugeType.EDGE;
            default:
                return HugeType.UNKNOWN;
        }
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    @Override
    public List<String> indexFields() {
        return this.indexFields;
    }

    public void indexFields(String... indexFields) {
        for (String field : indexFields) {
            if (!this.indexFields.contains(field)) {
                this.indexFields.add(field);
            }
        }
    }

    @Override
    public IndexLabel by(String... indexFields) {
        for (String field : indexFields) {
            if (!this.indexFields.contains(field)) {
                this.indexFields.add(field);
            }
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

    public void checkExist(boolean checkExists) {
        this.checkExist = checkExists;
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.makeIndexlabel(\"").append(this.name).append("\")");
        sb.append(this.baseLabelSchema());
        sb.append(this.indexFieldsSchema());
        sb.append(this.indexType.schema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    // TODO: Print the element name instead of object may lead custom confused.
    private String baseLabelSchema() {
        return String.format(".on(%s)", this.baseValue);
    }

    private String indexFieldsSchema() {
        StringBuilder sb = new StringBuilder();
        for (String indexField : this.indexFields) {
            sb.append("\"").append(indexField).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".by(%s)", sb.substring(0, endIdx));
    }

    @Override
    public IndexLabel create() {

        StringUtil.checkName(this.name);
        IndexLabel indexLabel = this.transaction().getIndexLabel(this.name);
        if (indexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("index label", this.name);
            } else {
                return indexLabel;
            }
        }

        // check field
        this.checkFields();

        if (this.element == null) {
            loadElement();
        }

        SchemaElement cloneElement = null;
        try {
            cloneElement = element.copy();
        } catch (CloneNotSupportedException e) {
            throw new NotAllowException("Not allowed to build index on this "
                    + "schema type: '%s'", this.element.type());
        }

        // TODO: should wrap update and add operation in one transaction.
        this.updateSchemaIndexName(cloneElement);

        this.transaction().addIndexLabel(this);

        // If addIndexLabel successed, update schema element indexNames
        this.element.indexNames(this.name);
        return this;
    }

    @Override
    public SchemaElement append() {
        throw new HugeException("Not support append operation for index label");
    }

    private void loadElement() {
        switch (this.baseType) {
            case VERTEX_LABEL:
                this.element = this.transaction().getVertexLabel(this.baseValue);
                break;
            case EDGE_LABEL:
                this.element = this.transaction().getEdgeLabel(this.baseValue);
                break;
            case PROPERTY_KEY:
                this.element = this.transaction().getPropertyKey(this.baseValue);
                break;
            default:
                throw new HugeException("Unsupported element type '%s' of " +
                          "index label '%s'", this.baseType, this.name);
        }
    }

    private void checkFields() {
        // search index must build on single numeric column
        if (this.indexType == IndexType.SEARCH) {
            E.checkArgument(this.indexFields.size() == 1,
                    "Search index can only build on one property, " +
                    "but got %s properties: '%s'",
                    this.indexFields.size(),
                    this.indexFields);
            String field = this.indexFields.iterator().next();
            PropertyKey propertyKey = this.transaction().getPropertyKey(field);
            E.checkArgument(
                    NumericUtil.isNumber(propertyKey.dataType().clazz()),
                    "Search index can only build on numeric property, " +
                    "but got %s(%s)", propertyKey.dataType(),
                    propertyKey.name());
        }

        for (String field : this.indexFields()) {
            PropertyKey propertyKey = this.transaction().getPropertyKey(field);
            E.checkArgument(propertyKey.cardinality() == Cardinality.SINGLE,
                    "Not allowed to build index on property key: '%s' that " +
                    "cardinality is list or set.", propertyKey.name());
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
                throw new HugeException("Can't update index name of " +
                                        "schema type: %s", baseType);
        }
    }

    @Override
    public void remove() {
        this.transaction().removeIndexLabel(this.name);
    }

    @Override
    protected SchemaElement copy() throws CloneNotSupportedException {
        throw new CloneNotSupportedException(
                "Not support copy operation for index label");
    }
}
