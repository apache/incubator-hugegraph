package com.baidu.hugegraph.type.define;

public enum HugeKeys {

    UNKNOWN(0, "undefined"),

    // column names of schema type (common)
    ID(1, "id"),
    NAME(2, "name"),
    TIMESTANMP(3, "timestamp"),

    // column names of schema type (VertexLabel)
    PROPERTIES(50, "properties"),
    PRIMARY_KEYS(51, "primaryKeys"),
    INDEX_NAMES(52, "indexNames"),

    // column names of schema type (EdgeLabel)
    LINKS(80, "links"),
    FREQUENCY(81, "frequency"),
    SRC_VERTEX_LABEL(82, "srcVertexLabel"),
    TGT_VERTEX_LABEL(83, "tgtVertexLabel"),
    SORT_KEYS(84, "sortKeys"),

    // column names of schema type (PropertyKey)
    DATA_TYPE(120, "dataType"),
    CARDINALITY(121, "cardinality"),

    // column names of schema type (IndexLabel)
    BASE_TYPE(150, "baseType"),
    BASE_VALUE(151, "baseValue"),
    INDEX_TYPE(152, "indexType"),
    FIELDS(153, "fields"),

    // column names of index data
    INDEX_NAME(180, "indexName"),
    PROPERTY_VALUES(181, "propertyValues"),
    INDEX_LABEL_NAME(182, "indexLabelName"),
    ELEMENT_IDS(183, "elementIds"),

    // column names of data type (Vertex/Edge)
    LABEL(200, "label"),
    SOURCE_VERTEX(201, "sourceVertex"),
    TARGET_VERTEX(202, "targetVertex"),
    PROPERTY_KEY(203, "propertyKey"),
    PROPERTY_VALUE(204, "propertyValue"),
    DIRECTION(205, "direction"),
    SORT_VALUES(206, "sortValues"),
    PRIMARY_VALUES(207, "primaryValues");

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
