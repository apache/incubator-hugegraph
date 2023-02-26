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

package org.apache.hugegraph.core;

import org.apache.hugegraph.election.StateMachineCallback;
import org.apache.hugegraph.election.StateMachineContext;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class StateMachineCallbackImpl implements StateMachineCallback {

    private static final Logger LOG = Log.logger(StateMachineCallbackImpl.class);

    private final TaskManager taskManager;

    boolean isMaster = false;

    public StateMachineCallbackImpl(TaskManager taskManager) {
        this.taskManager = taskManager;
        this.taskManager.useRoleStateMachine(true);
    }
    @Override
    public void master(StateMachineContext context) {
        if (!isMaster) {
            this.taskManager.onMaster();
            LOG.info("Server {} change to master role", context.config().node());
        }
        this.isMaster = true;
    }

    @Override
    public void worker(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }

        this.isMaster = false;
    }

    @Override
    public void candidate(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void unknown(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void abdication(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void error(StateMachineContext context, Throwable e) {
        LOG.error("Server {} exception occurred", context.config().node(), e);
    }
}
