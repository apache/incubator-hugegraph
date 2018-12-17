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

import com.baidu.hugegraph.unit.event.EventHubTest;
import com.baidu.hugegraph.unit.iterator.ExtendableIteratorTest;
import com.baidu.hugegraph.unit.iterator.FilterIteratorTest;
import com.baidu.hugegraph.unit.iterator.FlatMapperFilterIteratorTest;
import com.baidu.hugegraph.unit.iterator.FlatMapperIteratorTest;
import com.baidu.hugegraph.unit.iterator.MapperIteratorTest;
import com.baidu.hugegraph.unit.util.BytesTest;
import com.baidu.hugegraph.unit.util.CollectionUtilTest;
import com.baidu.hugegraph.unit.util.HashUtilTest;
import com.baidu.hugegraph.unit.util.VersionUtilTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    EventHubTest.class,

    ExtendableIteratorTest.class,
    FilterIteratorTest.class,
    MapperIteratorTest.class,
    FlatMapperIteratorTest.class,
    FlatMapperFilterIteratorTest.class,

    BytesTest.class,
    CollectionUtilTest.class,
    HashUtilTest.class,
    VersionUtilTest.class
})
public class UnitTestSuite {
}
