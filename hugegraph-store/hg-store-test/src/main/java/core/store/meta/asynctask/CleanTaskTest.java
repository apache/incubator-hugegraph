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

package core.store.meta.asynctask;

import static org.junit.Assert.assertEquals;

import org.apache.hugegraph.store.meta.asynctask.AbstractAsyncTask;
import org.apache.hugegraph.store.meta.asynctask.AsyncTask;
import org.apache.hugegraph.store.meta.asynctask.AsyncTaskState;
import org.apache.hugegraph.store.meta.asynctask.CleanTask;
import org.junit.Test;

import core.StoreEngineTestBase;

public class CleanTaskTest extends StoreEngineTestBase {

    @Test
    public void testSerialize() {
        CleanTask task = new CleanTask(0, "graph0", AsyncTaskState.SUCCESS, null);
        byte[] bytes = task.toBytes();

        AsyncTask task2 = AbstractAsyncTask.fromBytes(bytes);
        assertEquals(CleanTask.class, task2.getClass());
        System.out.println(task2);

        createPartitionEngine(0);

        CleanTask task3 = new CleanTask(0, "graph0", AsyncTaskState.START, null);
        CleanTask task4 = new CleanTask(0, "graph0", AsyncTaskState.FAILED, null);
        task3.handleTask();
        task4.handleTask();
    }

}
