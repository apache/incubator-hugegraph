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

package core.store.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.TaskManager;
import org.apache.hugegraph.store.meta.asynctask.AsyncTaskState;
import org.apache.hugegraph.store.meta.asynctask.CleanTask;
import org.junit.Before;
import org.junit.Test;

import core.StoreEngineTestBase;

public class TaskManagerTest extends StoreEngineTestBase {

    private TaskManager manager;

    @Before
    public void setup() {
        createPartitionEngine(0, "graph0");
        manager = getStoreEngine().getPartitionEngine(0).getTaskManager();
    }

    @Test
    public void testAsyncTaskReadAndWrite() {
        var task1 = new CleanTask(0, "graph0", AsyncTaskState.START, null);
        var task2 = new CleanTask(0, "graph0", AsyncTaskState.FAILED, null);
        var task3 = new CleanTask(0, "graph0", AsyncTaskState.SUCCESS, null);
        var task4 = new CleanTask(0, "graph0", AsyncTaskState.FAILED, null);

        manager.putAsyncTask(task1);
        manager.putAsyncTask(task2);
        manager.putAsyncTask(task3);
        manager.putAsyncTask(task4);

        var list = manager.scanAsyncTasks(0, "graph0");
        assertEquals(list.size(), 4);

        var newTask1 = (CleanTask) manager.getOneAsyncTask(0, "graph0", task1.getId());
        assertEquals(task1.getState(), newTask1.getState());
        assertEquals(task1.getType(), newTask1.getType());

        manager.updateAsyncTaskState(0, "graph0", task4.getId(), AsyncTaskState.SUCCESS);
        var newTask4 = (CleanTask) manager.getOneAsyncTask(0, "graph0", task4.getId());
        assertEquals(newTask4.getState(), AsyncTaskState.SUCCESS);

        assertNull(manager.getOneAsyncTask(1, "graph0", ""));
    }

    @Test
    public void testTaskOp() {
        MetaTask.Task task1 = MetaTask.Task.newBuilder()
                                           .setId(1)
                                           .setState(MetaTask.TaskState.Task_Ready)
                                           .setType(MetaTask.TaskType.Split_Partition)
                                           .setPartition(Metapb.Partition.newBuilder()
                                                                         .setGraphName("graph0")
                                                                         .setId(0).build())
                                           .build();

        manager.updateTask(task1);
        assertTrue(manager.taskExists(task1));
        assertFalse(manager.taskExists(0, "graph0",
                                       MetaTask.TaskType.Split_Partition.name()));
        assertFalse(manager.partitionTaskRepeat(0, "graph0",
                                                MetaTask.TaskType.Split_Partition.name()));

        MetaTask.Task task2 = MetaTask.Task.newBuilder(task1).setId(2).build();
        manager.updateTask(task2);

        assertTrue(manager.taskExists(0, "graph0",
                                      MetaTask.TaskType.Split_Partition.name()));
        assertTrue(manager.partitionTaskRepeat(0, "graph0",
                                               MetaTask.TaskType.Split_Partition.name()));

        MetaTask.Task task3 = MetaTask.Task.newBuilder(task1)
                                           .setId(3)
                                           .setState(MetaTask.TaskState.Task_Success)
                                           .setPartition(
                                                   Metapb.Partition.newBuilder(task1.getPartition())
                                                                   .setGraphName("graph1")
                                                                   .setId(1)
                                                                   .build())
                                           .build();
        manager.updateTask(task3);
        assertFalse(manager.taskExists(task3));
    }
}
