package com.baidu.hugegraph.type.define;

public enum HugeKeys {

    UNKNOWN(0, "undefined"),

    // column names of schema type (common)
    ID(1, "id"),
    NAME(2, "name"),
    TIMESTANMP(3, "timestamp"),

    // column names of schema type (VertexLabel)
    PROPERTIES(50, "properties"),
    PARTITION_KEYS(51, "partitionKeys"),
    CLUSTERING_KEYS(52, "clusteringKeys"),
    INDEX_TYPE(53, "indexType"),
    INDEX_NAME(54, "indexName"),
    INDEX_MAP(55, "indexMap"),
    PRIMARY_KEYS(56, "primaryKeys"),

    // column names of schema type (EdgeLabel)
    MULTIPLICITY(101, "multiplicity"),
    CARDINALITY(102, "cardinality"),
    SRC_VERTEX_LABEL(103, "srcVertexLabel"),
    TGT_VERTEX_LABEL(104, "tgtVertexLabel"),
    SORT_KEYS(105, "sortKeys"),
    LINKS(106, "links"),
    FREQUENCY(107, "frequency"),

    // column names of schema type (PropertyKey)
    DATA_TYPE(150, "dataType"),

    // column names of data type (Vertex/Edge)
    LABEL(200, "label"),
    SOURCE_VERTEX(201, "sourceVertex"),
    TARGET_VERTEX(202, "targetVertex"),
    PROPERTY_KEY(203, "propertyKey"),
    PROPERTY_VALUE(204, "propertyValue"),
    DIRECTION(205, "direction"),
    SORT_VALUES(206, "sortValues");

    // HugeKeys define
    private byte code = 0;
    private String name = null;

    private HugeKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
