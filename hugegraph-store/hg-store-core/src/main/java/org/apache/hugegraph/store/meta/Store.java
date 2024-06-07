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

package org.apache.hugegraph.store.meta;

import java.util.Map;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.store.util.Version;

import lombok.Data;

@Data
public class Store {

    private final String version;
    private long id = 0;
    private String storeAddress;
    private String pdAddress;
    private String raftAddress;
    private String deployPath;
    private String dataPath; // 数据存储路径
    private int dataVersion;
    private int partitionCount;
    private int startTime;
    private int usedSize;     //rocksdb存储大小
    private int pdHeartbeatInterval;
    private Metapb.StoreState state;
    private Map<String, String> labels;
    private int cores;

    public Store() {
        this.id = 0;
        this.version = Version.getVersion();
    }

    public Store(int dataVersion) {
        this.id = 0;
        this.dataVersion = dataVersion;
        this.version = Version.getVersion();
    }

    public Store(Metapb.Store protoObj) {
        if (protoObj != null) {
            this.id = protoObj.getId();
            this.raftAddress = protoObj.getRaftAddress();
            this.storeAddress = protoObj.getAddress();
            this.dataVersion = protoObj.getDataVersion();
        } else {
            this.id = 0;
        }
        this.version = Version.getVersion();
    }

    public Metapb.Store getProtoObj() {
        Asserts.isNonNull(storeAddress);
        Asserts.isNonNull(raftAddress);
        Metapb.Store.Builder builder = Metapb.Store.newBuilder()
                                                   .setId(id).setVersion(version)
                                                   .setDataVersion(dataVersion)
                                                   .setAddress(storeAddress)
                                                   .setRaftAddress(raftAddress)
                                                   .setState(Metapb.StoreState.Up)
                                                   .setCores(cores)
                                                   .setDeployPath(deployPath)
                                                   .setDataPath(dataPath);

        if (labels != null) {
            labels.forEach((k, v) -> {
                builder.addLabels(Metapb.StoreLabel.newBuilder().setKey(k).setValue(v).build());
            });
        }

        return builder.build();

    }

    public boolean checkState(Metapb.StoreState state) {
        return this.state == state;
    }
}
