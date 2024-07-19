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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_TASK;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_TASK_LOCK;

import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.lock.LockResult;

public class TaskMetaManager extends AbstractMetaManager {

    private static final String TASK_STATUS_POSTFIX = "Status";
    private static final String TASK_PROGRESS_POSTFIX = "Progress";
    private static final String TASK_CONTEXT_POSTFIX = "Context";
    private static final String TASK_RETRY_POSTFIX = "Retry";

    public TaskMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    public LockResult tryLockTask(String graphSpace, String graphName,
                                  String taskId) {
        String key = taskLockKey(graphSpace, graphName, taskId);
        return this.tryLock(key);
    }

    public boolean isLockedTask(String graphSpace, String graphName,
                                String taskId) {

        String key = taskLockKey(graphSpace, graphName, taskId);
        // 判断当前任务是否锁定
        return metaDriver.isLocked(key);
    }

    public void unlockTask(String graphSpace, String graphName,
                           String taskId, LockResult lockResult) {

        String key = taskLockKey(graphSpace, graphName, taskId);

        this.unlock(key, lockResult);
    }

    private String taskLockKey(String graphSpace,
                               String graphName,
                               String taskId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK/{id}/TASK_LOCK
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graphName,
                           META_PATH_TASK,
                           taskId,
                           META_PATH_TASK_LOCK);
    }
}
