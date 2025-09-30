/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store.jvector;

import org.apache.hugegraph.backend.store.BackendFeatures;

/**
 * Vector backend features
 */
public class VectorFeatures implements BackendFeatures {

    @Override
    public boolean supportsSharedStorage() {
        return false;
    }

    @Override
    public boolean supportsScanToken() {
        return false;
    }

    public boolean supportsDistributed() {
        return false;
    }


    public boolean supportsScan() {
        return true;
    }


    public boolean supportsScanKey() {
        return true;
    }

    @Override
    public boolean supportsScanKeyPrefix() {
        return true;
    }

    @Override
    public boolean supportsScanKeyRange() {
        return true;
    }

    @Override
    public boolean supportsQuerySchemaByName() {
        return false;
    }

    @Override
    public boolean supportsQueryByLabel() {
        return false;
    }

    @Override
    public boolean supportsQueryWithInCondition() {
        return false;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return false;
    }

    @Override
    public boolean supportsQueryWithOrderBy() {
        return false;
    }

    @Override
    public boolean supportsQueryWithContains() {
        return false;
    }

    @Override
    public boolean supportsQueryWithContainsKey() {
        return false;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsQuerySortByInputIds() {
        return false;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        return false;
    }

    @Override
    public boolean supportsUpdateVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsMergeVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsUpdateEdgeProperty() {
        return false;
    }

    @Override
    public boolean supportsTransaction() {
        return false;
    }

    @Override
    public boolean supportsNumberType() {
        return false;
    }

    @Override
    public boolean supportsAggregateProperty() {
        return false;
    }

    @Override
    public boolean supportsTtl() {
        return false;
    }

    @Override
    public boolean supportsOlapProperties() {
        return false;
    }

    @Override
    public boolean supportsVectorIndex() {
        return true;
    }
}
