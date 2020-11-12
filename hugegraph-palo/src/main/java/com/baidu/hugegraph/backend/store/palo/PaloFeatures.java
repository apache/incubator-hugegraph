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

package com.baidu.hugegraph.backend.store.palo;

import com.baidu.hugegraph.backend.store.BackendFeatures;

public class PaloFeatures implements BackendFeatures {

    @Override
    public boolean supportsScanToken() {
        return false;
    }

    @Override
    public boolean supportsScanKeyPrefix() {
        return false;
    }

    @Override
    public boolean supportsScanKeyRange() {
        return false;
    }

    @Override
    public boolean supportsQuerySchemaByName() {
        return true;
    }

    @Override
    public boolean supportsQueryByLabel() {
        /*
         * Create a rollup table on vertices/edges can speed up query by label,
         * but it will store data in vertices/edges again.
         * See: https://github.com/baidu/palo/wiki/Data-Model%2C-Rollup-%26
         * -Prefix-Index
         */
        return false;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return true;
    }

    @Override
    public boolean supportsQuerySortByInputIds() {
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
    public boolean supportsQueryWithOrderBy() {
        return true;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        return true;
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
        return true;
    }

    @Override
    public boolean supportsAggregateProperty() {
        return false;
    }

    @Override
    public boolean supportsTtl() {
        return false;
    }
}
