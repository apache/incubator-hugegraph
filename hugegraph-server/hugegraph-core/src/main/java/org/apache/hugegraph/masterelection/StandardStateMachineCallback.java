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

package org.apache.hugegraph.masterelection;

import java.util.Objects;

import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class StandardStateMachineCallback implements StateMachineCallback {

    private static final Logger LOG = Log.logger(StandardStateMachineCallback.class);

    private final TaskManager taskManager;

    private final GlobalMasterInfo globalMasterInfo;

    private boolean isMaster = false;

    public StandardStateMachineCallback(TaskManager taskManager, 
                                        GlobalMasterInfo globalMasterInfo) {
        this.taskManager = taskManager;
        this.taskManager.enableRoleElected(true);
        this.globalMasterInfo = globalMasterInfo;
        this.globalMasterInfo.isFeatureSupport(true);
    }

    @Override
    public void onAsRoleMaster(StateMachineContext context) {
        if (!isMaster) {
            this.taskManager.onAsRoleMaster();
            LOG.info("Server {} change to master role", context.config().node());
        }
        this.initGlobalMasterInfo(context);
        this.isMaster = true;
    }

    @Override
    public void onAsRoleWorker(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.initGlobalMasterInfo(context);
        this.isMaster = false;
    }

    @Override
    public void onAsRoleCandidate(StateMachineContext context) {
    }

    @Override
    public void unknown(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.initGlobalMasterInfo(context);

        isMaster = false;
    }

    @Override
    public void onAsRoleAbdication(StateMachineContext context) {
        if (isMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.initGlobalMasterInfo(context);

        isMaster = false;
    }

    @Override
    public void error(StateMachineContext context, Throwable e) {
        LOG.error("Server {} exception occurred", context.config().node(), e);
    }

    public void initGlobalMasterInfo(StateMachineContext context) {
        StateMachineContext.MasterServerInfo master = context.master();
        if (master == null) {
            this.globalMasterInfo.nodeInfo(false, "");
            return;
        }

        boolean isMaster = Objects.equals(context.node(), master.node());
        String url = master.url();
        this.globalMasterInfo.nodeInfo(isMaster, url);
    }
}
