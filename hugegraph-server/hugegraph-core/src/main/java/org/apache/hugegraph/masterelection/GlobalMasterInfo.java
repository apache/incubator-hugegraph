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

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.E;

// TODO: rename to GlobalNodeRoleInfo
public final class GlobalMasterInfo {

    private final static NodeInfo NO_MASTER = new NodeInfo(false, "");

    private volatile boolean supportElection;
    private volatile NodeInfo masterNodeInfo;

    private volatile Id serverId;
    private volatile NodeRole serverRole;

    public GlobalMasterInfo() {
        this(NO_MASTER);
    }

    public GlobalMasterInfo(NodeInfo masterInfo) {
        this.supportElection = false;
        this.masterNodeInfo = masterInfo;

        this.serverId = null;
        this.serverRole = null;
    }

    public void supportElection(boolean featureSupport) {
        this.supportElection = featureSupport;
    }

    public boolean supportElection() {
        return this.supportElection;
    }

    public void resetMasterInfo() {
        this.masterNodeInfo = NO_MASTER;
    }

    public void masterInfo(boolean isMaster, String nodeUrl) {
        // final can avoid instruction rearrangement, visibility can be ignored
        this.masterNodeInfo = new NodeInfo(isMaster, nodeUrl);
    }

    public NodeInfo masterInfo() {
        return this.masterNodeInfo;
    }

    public Id serverId() {
        return this.serverId;
    }

    public NodeRole serverRole() {
        return this.serverRole;
    }

    public void serverId(Id id) {
        this.serverId = id;
    }

    public void initServerRole(NodeRole role) {
        E.checkArgument(role != null, "The server role can't be null");
        E.checkArgument(this.serverRole == null,
                        "The server role can't be init twice");
        this.serverRole = role;
    }

    public void changeServerRole(NodeRole role) {
        E.checkArgument(role != null, "The server role can't be null");
        this.serverRole = role;
    }

    public static GlobalMasterInfo master(String serverId) {
        NodeInfo masterInfo = new NodeInfo(true, serverId);
        GlobalMasterInfo serverInfo = new GlobalMasterInfo(masterInfo);
        serverInfo.serverId = IdGenerator.of(serverId);
        serverInfo.serverRole = NodeRole.MASTER;
        return serverInfo;
    }

    public static class NodeInfo {

        private final boolean isMaster;
        private final String nodeUrl;

        public NodeInfo(boolean isMaster, String url) {
            this.isMaster = isMaster;
            this.nodeUrl = url;
        }

        public boolean isMaster() {
            return this.isMaster;
        }

        public String nodeUrl() {
            return this.nodeUrl;
        }
    }
}
