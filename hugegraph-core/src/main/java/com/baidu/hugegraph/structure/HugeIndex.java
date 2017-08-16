/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.baidu.hugegraph.structure;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;


public class HugeIndex implements GraphType {

    private Object fieldValues = null;
    private final IndexLabel label;
    private final Set<Id> elementIds;

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

    public Id id() {
        String propValues = fieldValues() == null ?
                            "<?>" : fieldValues().toString();
        if (type() == HugeType.SECONDARY_INDEX) {
            return SplicingIdGenerator.splicing(propValues, indexLabelName());
        } else {
            assert type() == HugeType.SEARCH_INDEX;
            return SplicingIdGenerator.splicing(indexLabelName(), propValues);
        }
    }

    public Object fieldValues() {
        return this.fieldValues;
    }

    public void fieldValues(Object fieldValues) {
        this.fieldValues = fieldValues;
    }

    public String indexLabelName() {
        return this.label.name();
    }

    public Set<Id> elementIds() {
        return Collections.unmodifiableSet(this.elementIds);
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
