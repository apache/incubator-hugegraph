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

import org.apache.hugegraph.pd.grpc.Metapb;

public enum PartitionRole {
    UNKNOWN(0, "unknown"),
    LEADER(1, "leader"),
    FOLLOWER(2, "follower"),
    LEARNER(3, "learner"),
    CANDIDATE(4, "candidate");
    private final int role;
    private final String name;

    PartitionRole(int role, String name) {
        this.role = role;
        this.name = name;
    }

    public static PartitionRole fromShardRole(Metapb.ShardRole shard) {
        PartitionRole role = PartitionRole.FOLLOWER;
        switch (shard) {
            case Leader:
                role = PartitionRole.LEADER;
                break;
            case Follower:
                role = PartitionRole.FOLLOWER;
                break;
            case Learner:
                role = PartitionRole.LEARNER;
                break;
        }
        return role;

    }

    @Override
    public String toString() {
        return this.ordinal() + "_" + this.name;
    }

    public String getName() {
        return this.name;
    }

    public Metapb.ShardRole toShardRole() {
        Metapb.ShardRole shardRole = Metapb.ShardRole.None;
        switch (this) {
            case LEADER:
                shardRole = Metapb.ShardRole.Leader;
                break;
            case FOLLOWER:
                shardRole = Metapb.ShardRole.Follower;
                break;
            case LEARNER:
                shardRole = Metapb.ShardRole.Learner;
                break;
        }
        return shardRole;
    }
}
