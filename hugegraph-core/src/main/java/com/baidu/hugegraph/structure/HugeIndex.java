package com.baidu.hugegraph.structure;

import java.util.Arrays;
import java.util.Set;

import com.baidu.hugegraph.type.define.IndexType;

/**
 * Created by liningrui on 2017/4/25.
 */
public class HugeIndex {

    private IndexType indexType;
    private String propertyValues;
    private String indexLabelId;
    private Set<String> elementIds;

    public HugeIndex(IndexType indexType) {
        this.indexType = indexType;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public String getPropertyValues() {
        return propertyValues;
    }

    public void setPropertyValues(String propertyValues) {
        this.propertyValues = propertyValues;
    }

    public String getIndexLabelId() {
        return indexLabelId;
    }

    public void setIndexLabelId(String indexLabelId) {
        this.indexLabelId = indexLabelId;
    }

    public Set<String> getElementIds() {
        return elementIds;
    }

    public void setElementIds(Set<String> elementIds) {
        this.elementIds = elementIds;
    }

    public void setElementIds(String... elementIds) {
        this.elementIds.addAll(Arrays.asList(elementIds));
    }

}
