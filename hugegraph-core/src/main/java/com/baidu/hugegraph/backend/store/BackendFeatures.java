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

package com.baidu.hugegraph.backend.store;

public interface BackendFeatures {

    public default boolean supportsPersistence() {
        return true;
    }

    public boolean supportsScanToken();

    public boolean supportsScanKeyPrefix();

    public boolean supportsScanKeyRange();

    public boolean supportsQuerySchemaByName();

    public boolean supportsQueryByLabel();

    public boolean supportsQueryWithRangeCondition();

    public boolean supportsQueryWithContains();

    public boolean supportsQueryWithContainsKey();

    public boolean supportsQueryWithOrderBy();

    public boolean supportsQueryByPage();

    public boolean supportsQuerySortByInputIds();

    public boolean supportsDeleteEdgeByLabel();

    public boolean supportsUpdateVertexProperty();

    public boolean supportsMergeVertexProperty();

    public boolean supportsUpdateEdgeProperty();

    public boolean supportsTransaction();

    public boolean supportsNumberType();

    public boolean supportsAggregateProperty();

    public boolean supportsTtl();
}
