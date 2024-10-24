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

package org.apache.hugegraph.ct.config;

import static org.apache.hugegraph.ct.base.ClusterConstant.CONFIG_FILE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.GRAPH_TEMPLATE_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.HUGEGRAPH_PROPERTIES;

import java.nio.file.Paths;
import java.util.List;

public class GraphConfig extends AbstractConfig {

    public GraphConfig() {
        readTemplate(Paths.get(CONFIG_FILE_PATH + GRAPH_TEMPLATE_FILE));
        this.fileName = HUGEGRAPH_PROPERTIES;
    }

    public void setPDPeersList(List<String> pdPeersList) {
        String pdPeers = String.join(",", pdPeersList);
        setProperty("PD_PEERS_LIST", pdPeers);
    }
}
