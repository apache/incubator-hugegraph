/*
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

package core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import core.raft.RaftOperationTest;
import core.raft.RaftUtilsTest;
import core.snapshot.HgSnapshotHandlerTest;
import core.store.HgStoreEngineTest;
import core.store.PartitionEngineTest;
import core.store.PartitionInstructionProcessorTest;
import core.store.meta.MetadataKeyHelperTest;
import core.store.meta.PartitionManagerTest;
import core.store.meta.TaskManagerTest;
import core.store.meta.asynctask.CleanTaskTest;
import core.store.util.MiscUtilClassTest;
import core.store.util.PartitionMetaStoreWrapperTest;
import core.store.util.ZipUtilsTest;
import lombok.extern.slf4j.Slf4j;
import util.CopyOnWriteCacheTest;
import util.UnsafeUtilTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HgCmdClientTest.class,
        HgSnapshotHandlerTest.class,
        RaftUtilsTest.class,
        RaftOperationTest.class,
        UnsafeUtilTest.class,
        CopyOnWriteCacheTest.class,
        MetricServiceTest.class,
        TaskManagerTest.class,
        CleanTaskTest.class,
        MetadataKeyHelperTest.class,
        HgStoreEngineTest.class,
        PartitionEngineTest.class,
        PartitionManagerTest.class,
        PartitionMetaStoreWrapperTest.class,
        ZipUtilsTest.class,
        MiscUtilClassTest.class,
        PartitionInstructionProcessorTest.class,
        // 尽量放到最后
        HgBusinessImplTest.class
})

@Slf4j
public class CoreSuiteTest {


}