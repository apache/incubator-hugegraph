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

import com.baidu.hugegraph.testutil.AssertTest;
import com.baidu.hugegraph.testutil.WhiteboxTest;
import com.baidu.hugegraph.unit.concurrent.AtomicLockTest;
import com.baidu.hugegraph.unit.concurrent.BarrierEventTest;
import com.baidu.hugegraph.unit.concurrent.KeyLockTest;
import com.baidu.hugegraph.unit.concurrent.LockGroupTest;
import com.baidu.hugegraph.unit.concurrent.LockManagerTest;
import com.baidu.hugegraph.unit.concurrent.PausableScheduledThreadPoolTest;
import com.baidu.hugegraph.unit.concurrent.RowLockTest;
import com.baidu.hugegraph.unit.config.HugeConfigTest;
import com.baidu.hugegraph.unit.config.OptionSpaceTest;
import com.baidu.hugegraph.unit.date.SafeDateFormatTest;
import com.baidu.hugegraph.unit.event.EventHubTest;
import com.baidu.hugegraph.unit.iterator.BatchMapperIteratorTest;
import com.baidu.hugegraph.unit.iterator.ExtendableIteratorTest;
import com.baidu.hugegraph.unit.iterator.FilterIteratorTest;
import com.baidu.hugegraph.unit.iterator.FlatMapperFilterIteratorTest;
import com.baidu.hugegraph.unit.iterator.FlatMapperIteratorTest;
import com.baidu.hugegraph.unit.iterator.LimitIteratorTest;
import com.baidu.hugegraph.unit.iterator.ListIteratorTest;
import com.baidu.hugegraph.unit.iterator.MapperIteratorTest;
import com.baidu.hugegraph.unit.license.LicenseExtraParamTest;
import com.baidu.hugegraph.unit.license.LicenseCreateParamTest;
import com.baidu.hugegraph.unit.license.LicenseInstallParamTest;
import com.baidu.hugegraph.unit.license.LicenseParamsTest;
import com.baidu.hugegraph.unit.license.MachineInfoTest;
import com.baidu.hugegraph.unit.perf.PerfUtilTest;
import com.baidu.hugegraph.unit.perf.StopwatchTest;
import com.baidu.hugegraph.unit.rest.RestClientTest;
import com.baidu.hugegraph.unit.rest.RestResultTest;
import com.baidu.hugegraph.unit.util.BytesTest;
import com.baidu.hugegraph.unit.util.CollectionUtilTest;
import com.baidu.hugegraph.unit.util.DateUtilTest;
import com.baidu.hugegraph.unit.util.EcheckTest;
import com.baidu.hugegraph.unit.util.HashUtilTest;
import com.baidu.hugegraph.unit.util.InsertionOrderUtilTest;
import com.baidu.hugegraph.unit.util.LogTest;
import com.baidu.hugegraph.unit.util.LongEncodingTest;
import com.baidu.hugegraph.unit.util.NumericUtilTest;
import com.baidu.hugegraph.unit.util.OrderLimitMapTest;
import com.baidu.hugegraph.unit.util.ReflectionUtilTest;
import com.baidu.hugegraph.unit.util.StringUtilTest;
import com.baidu.hugegraph.unit.util.TimeUtilTest;
import com.baidu.hugegraph.unit.util.UnitUtilTest;
import com.baidu.hugegraph.unit.util.VersionUtilTest;
import com.baidu.hugegraph.unit.version.VersionTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    LockManagerTest.class,
    LockGroupTest.class,
    AtomicLockTest.class,
    KeyLockTest.class,
    RowLockTest.class,
    PausableScheduledThreadPoolTest.class,

    HugeConfigTest.class,
    OptionSpaceTest.class,
    SafeDateFormatTest.class,
    BarrierEventTest.class,
    EventHubTest.class,
    PerfUtilTest.class,
    StopwatchTest.class,
    RestClientTest.class,
    RestResultTest.class,
    VersionTest.class,

    ExtendableIteratorTest.class,
    FilterIteratorTest.class,
    LimitIteratorTest.class,
    MapperIteratorTest.class,
    FlatMapperIteratorTest.class,
    FlatMapperFilterIteratorTest.class,
    ListIteratorTest.class,
    BatchMapperIteratorTest.class,

    BytesTest.class,
    CollectionUtilTest.class,
    EcheckTest.class,
    HashUtilTest.class,
    InsertionOrderUtilTest.class,
    LogTest.class,
    NumericUtilTest.class,
    ReflectionUtilTest.class,
    StringUtilTest.class,
    TimeUtilTest.class,
    VersionUtilTest.class,
    LongEncodingTest.class,
    OrderLimitMapTest.class,
    DateUtilTest.class,
    UnitUtilTest.class,

    LicenseExtraParamTest.class,
    LicenseCreateParamTest.class,
    LicenseInstallParamTest.class,
    LicenseParamsTest.class,
    MachineInfoTest.class,

    AssertTest.class,
    WhiteboxTest.class
})
public class UnitTestSuite {
}
