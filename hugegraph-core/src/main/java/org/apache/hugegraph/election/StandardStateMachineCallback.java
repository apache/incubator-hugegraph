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

package org.apache.hugegraph.election;

import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Objects;

public class StandardStateMachineCallback implements StateMachineCallback {

    private static final Logger LOG = Log.logger(StandardStateMachineCallback.class);

    private final TaskManager taskManager;

    private boolean isMaster = false;

    public StandardStateMachineCallback(TaskManager taskManager) {
        this.taskManager = taskManager;
        this.taskManager.enableRoleElected(true);
        GlobalMasterInfo.instance().isFeatureSupport(true);
    }

    @Override
    public void onAsRoleMaster(StateMachineContext context) {
        if (!isMaster) {
            this.taskManager.onAsRoleMaster();
            this.initGlobalMasterInfo(context);
            LOG.info("Server {} change to master role", context.config().node());
        }
        this.isMaster = true;
    }

    @Override
    public void onAsRoleWorker(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            this.initGlobalMasterInfo(context);
            LOG.info("Server {} change to worker role", context.config().node());
        }

        this.isMaster = false;
    }

    @Override
    public void onAsRoleCandidate(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            this.initGlobalMasterInfo(context);
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void unknown(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            this.initGlobalMasterInfo(context);
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void onAsRoleAbdication(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            this.initGlobalMasterInfo(context);
            LOG.info("Server {} change to worker role", context.config().node());
        }

        isMaster = false;
    }

    @Override
    public void error(StateMachineContext context, Throwable e) {
        LOG.error("Server {} exception occurred", context.config().node(), e);
    }

    public void initGlobalMasterInfo(StateMachineContext context) {
        StateMachineContext.MasterServerInfo master = context.master();
        if (master == null) {
            GlobalMasterInfo.instance().set(false, null);
            return;
        }

        boolean isMaster = Objects.equals(context.node(), master.node());
        String url = master.url();
        GlobalMasterInfo.instance().set(isMaster, url);
    }
}
