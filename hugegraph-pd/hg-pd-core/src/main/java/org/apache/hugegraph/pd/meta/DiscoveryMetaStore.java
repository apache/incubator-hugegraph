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

package org.apache.hugegraph.pd.meta;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;

import lombok.extern.slf4j.Slf4j;

@Useless("discovery related")
@Slf4j
public class DiscoveryMetaStore extends MetadataRocksDBStore {

    /**
     * appName --> address --> registryInfo
     */
    private static final String PREFIX = "REGIS-";
    private static final String SPLITTER = "-";

    public DiscoveryMetaStore(PDConfig pdConfig) {
        super(pdConfig);
    }

    public void register(NodeInfo nodeInfo, int outTimes) throws PDException {
        putWithTTL(toKey(nodeInfo.getAppName(), nodeInfo.getVersion(), nodeInfo.getAddress()),
                   nodeInfo.toByteArray(), (nodeInfo.getInterval() / 1000) * outTimes);
    }

    byte[] toKey(String appName, String version, String address) {
        StringBuilder builder = getPrefixBuilder(appName, version);
        builder.append(SPLITTER);
        builder.append(address);
        return builder.toString().getBytes();
    }

    private StringBuilder getPrefixBuilder(String appName, String version) {
        StringBuilder builder = new StringBuilder();
        builder.append(PREFIX);
        if (!StringUtils.isEmpty(appName)) {
            builder.append(appName);
            builder.append(SPLITTER);
        }
        if (!StringUtils.isEmpty(version)) {
            builder.append(version);
        }
        return builder;
    }

    public NodeInfos getNodes(Query query) {
        List<NodeInfo> nodeInfos = null;
        try {
            StringBuilder builder = getPrefixBuilder(query.getAppName(),
                                                     query.getVersion());
            nodeInfos = getInstanceListWithTTL(
                    NodeInfo.parser(),
                    builder.toString().getBytes());
            builder.setLength(0);
        } catch (PDException e) {
            log.error("An error occurred getting data from the store,{}", e);
        }
        if (query.getLabelsMap() != null && !query.getLabelsMap().isEmpty()) {
            List result = new LinkedList<NodeInfo>();
            for (NodeInfo node : nodeInfos) {
                if (labelMatch(node, query)) {
                    result.add(node);
                }
            }
            return NodeInfos.newBuilder().addAllInfo(result).build();
        }
        return NodeInfos.newBuilder().addAllInfo(nodeInfos).build();

    }

    private boolean labelMatch(NodeInfo node, Query query) {
        Map<String, String> labelsMap = node.getLabelsMap();
        for (Map.Entry<String, String> entry : query.getLabelsMap().entrySet()) {
            if (!entry.getValue().equals(labelsMap.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
