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

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.Data;

@Data
public class PartitionStats {

    // Partition leader in shard
    Metapb.Shard leader;
    // Partition offline shard
    List<Metapb.Shard> offlineShards = new ArrayList<>();
    long committedIndex;
    long leaderTerm;
    long approximateSize;
    long approximateKeys;
    // Partition ID
    private int id;
    private String namespace;
    private String graphName;

    public PartitionStats addOfflineShard(Metapb.Shard shard) {
        offlineShards.add(shard);
        return this;
    }

    public Metapb.PartitionStats getProtoObj() {
        return Metapb.PartitionStats.newBuilder()
                                    .setId(id)
                                    .addGraphName(graphName)
                                    .setLeader(leader)
                                    .addAllShard(offlineShards)
                                    .setApproximateKeys(approximateKeys)
                                    .setApproximateSize(approximateSize)
                                    .setLeaderTerm(leaderTerm)
                                    .build();
    }
}
