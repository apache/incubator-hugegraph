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

package com.baidu.hugegraph.backend.store.hbase;

import com.baidu.hugegraph.backend.store.BackendFeatures;

public class HbaseFeatures implements BackendFeatures {

    @Override
    public boolean supportsScanToken() {
        return false;
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
        // TODO: Supports this feature through HBase secondary index
        return false;
    }

    @Override
    public boolean supportsQueryByLabel() {
        // TODO: Supports this feature through HBase secondary index
        return false;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return true;
    }

    @Override
    public boolean supportsQueryWithOrderBy() {
        return true;
    }

    @Override
    public boolean supportsQueryWithContains() {
        // TODO: Need to traversal all items
        return false;
    }

    @Override
    public boolean supportsQueryWithContainsKey() {
        // TODO: Need to traversal all items
        return false;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        // TODO: Supports this feature through HBase secondary index
        return false;
    }

    @Override
    public boolean supportsUpdateVertexProperty() {
        return true;
    }

    @Override
    public boolean supportsMergeVertexProperty() {
        return true;
    }

    @Override
    public boolean supportsUpdateEdgeProperty() {
        // Edge properties are stored in a cell(column value)
        return false;
    }

    @Override
    public boolean supportsTransaction() {
        // TODO: Supports tx through BufferedMutator and range-lock
        return false;
    }

    @Override
    public boolean supportsNumberType() {
        return false;
    }
}
