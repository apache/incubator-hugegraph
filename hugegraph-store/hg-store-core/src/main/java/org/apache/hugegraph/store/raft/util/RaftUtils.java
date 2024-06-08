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

package org.apache.hugegraph.store.raft.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.ListUtils;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;

public class RaftUtils {

    public static List<String> getAllEndpoints(Node node) {
        List<String> endPoints = new ArrayList<>();
        node.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        node.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getAllEndpoints(Configuration conf) {
        List<String> endPoints = new ArrayList<>();
        conf.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        conf.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getPeerEndpoints(Node node) {
        List<String> endPoints = new ArrayList<>();
        node.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getPeerEndpoints(Configuration conf) {
        List<String> endPoints = new ArrayList<>();
        conf.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getLearnerEndpoints(Node node) {
        List<String> endPoints = new ArrayList<>();
        node.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getLearnerEndpoints(Configuration conf) {
        List<String> endPoints = new ArrayList<>();
        conf.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static boolean configurationEquals(Configuration oldConf, Configuration newConf) {
        return ListUtils.isEqualList(oldConf.listPeers(), newConf.listPeers()) &&
               ListUtils.isEqualList(oldConf.listLearners(), newConf.listLearners());
    }
}
