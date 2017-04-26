package com.baidu.hugegraph.structure;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.IndexLabel;

/**
 * Created by liningrui on 2017/4/25.
 */
public class HugeIndex {

    private IndexLabel indexLabel;
    private String propertyValues;
    private Set<Id> elementIds;

    public HugeIndex(IndexLabel indexLabel) {
        this.indexLabel = indexLabel;
        this.elementIds = new LinkedHashSet<>();
    }

    public String id() {
        if (indexType() == IndexType.SECONDARY) {
            return propertyValues() + indexLabelName() + elementIds;
        } else {
            return indexLabelName() + propertyValues() + elementIds;
        }
    }

    public IndexType indexType() {
        return this.indexLabel.indexType();
    }

    public String propertyValues() {
        return this.propertyValues;
    }

    public void propertyValues(String propertyValues) {
        this.propertyValues = propertyValues;
    }

    public String indexLabelName() {
        return this.indexLabel.name();
    }

    public Set<Id> elementIds() {
        return elementIds;
    }

    public void elementIds(Set<Id> elementIds) {
        this.elementIds = elementIds;
    }

    public void elementIds(Id... elementIds) {
        this.elementIds.addAll(Arrays.asList(elementIds));
    }

}
