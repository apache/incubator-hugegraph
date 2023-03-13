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

public class GlobalMasterInfo {

    private NodeInfo nodeInfo;
    private volatile boolean featureSupport;

    public GlobalMasterInfo() {
        this.featureSupport = false;
        this.nodeInfo = new NodeInfo(false, "");
    }

    public final void nodeInfo(boolean isMaster, String url) {
        // final can avoid instruction rearrangement, visibility can be ignored
        final NodeInfo tmp = new NodeInfo(isMaster, url);
        this.nodeInfo = tmp;
    }

    public final NodeInfo nodeInfo() {
        return this.nodeInfo;
    }

    public void isFeatureSupport(boolean featureSupport) {
        this.featureSupport = featureSupport;
    }

    public boolean isFeatureSupport() {
        return this.featureSupport;
    }

    public static class NodeInfo {

        private final boolean isMaster;
        private final String url;

        public NodeInfo(boolean isMaster, String url) {
            this.isMaster = isMaster;
            this.url = url;
        }

        public boolean isMaster() {
            return this.isMaster;
        }

        public String url() {
            return this.url;
        }
    }
}
