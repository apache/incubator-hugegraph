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

package org.apache.hugegraph.backend.store;

public interface BackendFeatures {

    default boolean supportsPersistence() {
        return true;
    }

    default boolean supportsSharedStorage() {
        return true;
    }

    default boolean supportsSnapshot() {
        return false;
    }

    default boolean supportsTaskAndServerVertex() { return false; }

    boolean supportsScanToken();

    boolean supportsScanKeyPrefix();

    boolean supportsScanKeyRange();

    boolean supportsQuerySchemaByName();

    boolean supportsQueryByLabel();

    boolean supportsQueryWithInCondition();

    boolean supportsQueryWithRangeCondition();

    boolean supportsQueryWithContains();

    boolean supportsQueryWithContainsKey();

    boolean supportsQueryWithOrderBy();

    boolean supportsQueryByPage();

    boolean supportsQuerySortByInputIds();

    boolean supportsDeleteEdgeByLabel();

    boolean supportsUpdateVertexProperty();

    boolean supportsMergeVertexProperty();

    boolean supportsUpdateEdgeProperty();

    boolean supportsTransaction();

    boolean supportsNumberType();

    boolean supportsAggregateProperty();

    boolean supportsTtl();

    boolean supportsOlapProperties();
}
