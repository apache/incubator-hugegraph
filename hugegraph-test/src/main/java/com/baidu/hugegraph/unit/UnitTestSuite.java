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

package com.baidu.hugegraph.unit;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.unit.cache.CacheManagerTest;
import com.baidu.hugegraph.unit.cache.CachedGraphTransactionTest;
import com.baidu.hugegraph.unit.cache.CachedSchemaTransactionTest;
import com.baidu.hugegraph.unit.cache.RamCacheTest;
import com.baidu.hugegraph.unit.cassandra.CassandraTest;
import com.baidu.hugegraph.unit.core.AnalyzerTest;
import com.baidu.hugegraph.unit.core.BackendMutationTest;
import com.baidu.hugegraph.unit.core.BackendStoreSystemInfoTest;
import com.baidu.hugegraph.unit.core.ConditionQueryFlattenTest;
import com.baidu.hugegraph.unit.core.ConditionTest;
import com.baidu.hugegraph.unit.core.DataTypeTest;
import com.baidu.hugegraph.unit.core.DirectionsTest;
import com.baidu.hugegraph.unit.core.EdgeIdTest;
import com.baidu.hugegraph.unit.core.ExceptionTest;
import com.baidu.hugegraph.unit.core.LocksTableTest;
import com.baidu.hugegraph.unit.core.QueryTest;
import com.baidu.hugegraph.unit.core.RowLockTest;
import com.baidu.hugegraph.unit.core.SecurityManagerTest;
import com.baidu.hugegraph.unit.core.SerialEnumTest;
import com.baidu.hugegraph.unit.id.IdTest;
import com.baidu.hugegraph.unit.id.IdUtilTest;
import com.baidu.hugegraph.unit.mysql.MysqlUtilTest;
import com.baidu.hugegraph.unit.mysql.WhereBuilderTest;
import com.baidu.hugegraph.unit.rocksdb.RocksDBCountersTest;
import com.baidu.hugegraph.unit.rocksdb.RocksDBSessionsTest;
import com.baidu.hugegraph.unit.serializer.BinaryBackendEntryTest;
import com.baidu.hugegraph.unit.serializer.BinaryScatterSerializerTest;
import com.baidu.hugegraph.unit.serializer.BinarySerializerTest;
import com.baidu.hugegraph.unit.serializer.BytesBufferTest;
import com.baidu.hugegraph.unit.serializer.SerializerFactoryTest;
import com.baidu.hugegraph.unit.serializer.TableBackendEntryTest;
import com.baidu.hugegraph.unit.serializer.TextBackendEntryTest;
import com.baidu.hugegraph.unit.util.JsonUtilTest;
import com.baidu.hugegraph.unit.util.StringEncodingTest;
import com.baidu.hugegraph.unit.util.VersionTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    /* cache */
    RamCacheTest.class,
    CachedSchemaTransactionTest.class,
    CachedGraphTransactionTest.class,
    CacheManagerTest.class,

    /* types */
    DataTypeTest.class,
    DirectionsTest.class,
    SerialEnumTest.class,

    /* id */
    IdTest.class,
    IdUtilTest.class,

    /* core */
    LocksTableTest.class,
    RowLockTest.class,
    AnalyzerTest.class,
    EdgeIdTest.class,
    BackendMutationTest.class,
    ConditionTest.class,
    ConditionQueryFlattenTest.class,
    QueryTest.class,
    SecurityManagerTest.class,
    ExceptionTest.class,
    BackendStoreSystemInfoTest.class,

    /* serializer */
    BytesBufferTest.class,
    SerializerFactoryTest.class,
    TextBackendEntryTest.class,
    TableBackendEntryTest.class,
    BinaryBackendEntryTest.class,
    BinarySerializerTest.class,
    BinaryScatterSerializerTest.class,

    /* cassandra */
    CassandraTest.class,

    /* mysql */
    MysqlUtilTest.class,
    WhereBuilderTest.class,

    /* rocksdb */
    RocksDBSessionsTest.class,
    RocksDBCountersTest.class,

    /* utils */
    VersionTest.class,
    JsonUtilTest.class,
    StringEncodingTest.class
})
public class UnitTestSuite {
}
