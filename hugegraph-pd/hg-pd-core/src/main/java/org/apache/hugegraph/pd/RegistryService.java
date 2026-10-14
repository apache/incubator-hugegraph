/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd;

import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.meta.DiscoveryMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;

public class RegistryService {

    // Shared across facades. Never acquired by the Raft apply thread.
    private static final Object REGISTRATION_MONITOR = new Object();
    private static final String SERVICE_ID = "SERVICE_ID";
    private static final String GRAPHSPACE = "GRAPHSPACE";
    private static final String SERVICE_NAME = "SERVICE_NAME";
    private final PDConfig pdConfig;
    private final DiscoveryMetaStore meta;

    public RegistryService(PDConfig config) {
        this.pdConfig = config;
        this.meta = MetadataFactory.newDiscoveryMeta(config);
    }

    public void register(NodeInfo node, int outTimes) throws PDException {
        synchronized (REGISTRATION_MONITOR) {
            long term = readTerm();
            NodeInfo previous = this.meta.registration(node);
            if (previous != null && complete(previous) && !complete(node)) {
                throw conflict();
            }
            if (node.containsLabels(SERVICE_ID)) {
                List<NodeInfo> pool = pool(node.getAppName(), node.getLabelsOrThrow(SERVICE_ID));
                boolean protectedPool = complete(node) || pool.stream().anyMatch(RegistryService::complete);
                if (protectedPool && (!complete(node) ||
                                      pool.stream().anyMatch(other -> !sameIdentity(node, other)))) {
                    throw conflict();
                }
            }
            this.meta.register(node, outTimes, term);
        }
    }

    public NodeInfos getNodes(Query query) {
        if (!query.containsLabels(SERVICE_ID)) {
            return this.meta.getNodes(query);
        }
        synchronized (REGISTRATION_MONITOR) {
            try {
                readTerm();
                List<NodeInfo> pool = pool(query.getAppName(), query.getLabelsOrThrow(SERVICE_ID));
                for (NodeInfo node : pool) {
                    if (complete(node) && pool.stream().anyMatch(other ->
                            node.getAppName().equals(other.getAppName()) && !sameIdentity(node, other))) {
                        throw conflict();
                    }
                }
                return this.meta.getNodesStrict(query);
            } catch (PDException e) {
                throw new PDRuntimeException(e.getErrorCode(), e.getMessage(), e);
            }
        }
    }

    private long readTerm() throws PDException {
        return this.pdConfig.getRaft().isEnable() ?
               RaftEngine.getInstance().discoveryReadTerm() : -1;
    }

    private List<NodeInfo> pool(String appName, String id) throws PDException {
        return this.meta.getNodesStrict(Query.newBuilder().setAppName(appName)
                                            .putLabels(SERVICE_ID, id).build()).getInfoList();
    }

    private static boolean complete(NodeInfo node) {
        return node.containsLabels(SERVICE_ID) && node.containsLabels(GRAPHSPACE) &&
               node.containsLabels(SERVICE_NAME);
    }

    private static boolean sameIdentity(NodeInfo first, NodeInfo second) {
        return complete(second) && Objects.equals(first.getLabelsOrThrow(GRAPHSPACE),
                                                  second.getLabelsOrThrow(GRAPHSPACE)) &&
               Objects.equals(first.getLabelsOrThrow(SERVICE_NAME), second.getLabelsOrThrow(SERVICE_NAME));
    }

    private static PDException conflict() {
        return new PDException(Pdpb.ErrorType.UNKNOWN_VALUE,
                "Discovery identity conflict: this SERVICE_ID pool contains an incomplete or different " +
                "GRAPHSPACE/SERVICE_NAME tuple. Stop conflicting registrations and wait for their TTL, " +
                "or explicitly select a different service identity.");
    }
}
