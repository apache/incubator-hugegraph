package com.baidu.hugegraph.structure;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.IndexLabel;

/**
 * Created by liningrui on 2017/4/25.
 */
public class HugeIndex implements GraphType {

    private IndexLabel label;
    private Object fieldValues;
    private Set<Id> elementIds;

    public HugeIndex(IndexLabel indexLabel) {
        this.label = indexLabel;
        this.elementIds = new LinkedHashSet<>();
    }

    @Override
    public String name() {
        return this.label.name();
    }

    @Override
    public HugeType type() {
        IndexType indexType = this.label.indexType();
        if (indexType == IndexType.SECONDARY) {
            return HugeType.SECONDARY_INDEX;
        } else {
            assert indexType == IndexType.SEARCH;
            return HugeType.SEARCH_INDEX;
        }
    }

    public String id() {
        if (type() == HugeType.SECONDARY_INDEX) {
            return propertyValues() + indexLabelName();
        } else {
            assert type() == HugeType.SEARCH_INDEX;
            return indexLabelName() + propertyValues();
        }
    }

    public Object propertyValues() {
        return this.fieldValues;
    }

    public void propertyValues(Object propertyValues) {
        this.fieldValues = propertyValues;
    }

    public String indexLabelName() {
        return this.label.name();
    }

    public Set<Id> elementIds() {
        return Collections.unmodifiableSet(this.elementIds);
    }

    public void elementIds(Set<Id> elementIds) {
        this.elementIds = elementIds;
    }

    public void elementIds(Id... elementIds) {
        this.elementIds.addAll(Arrays.asList(elementIds));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeIndex)) {
            return false;
        }

        HugeIndex other = (HugeIndex) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public String toString() {
        return String.format("{label=%s<%s>, fieldValues=%s, elementIds=%s}",
                             this.label.name(), this.label.indexType().string(),
                             this.fieldValues, this.elementIds);
    }
}
