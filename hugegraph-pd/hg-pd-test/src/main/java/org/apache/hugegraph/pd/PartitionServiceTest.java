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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Test;

@Useless // can be merged to org.apache.hugegraph.pd.service.PartitionServiceTest
public class PartitionServiceTest {
    @Test
    public void testPartitionHeartbeat() {
        List<Metapb.Shard> shardList = new ArrayList<>();
        shardList.add(Metapb.Shard.newBuilder().setStoreId(1).build());
        shardList.add(Metapb.Shard.newBuilder().setStoreId(2).build());
        shardList.add(Metapb.Shard.newBuilder().setStoreId(3).build());
        shardList = new ArrayList<>(shardList);
        Metapb.PartitionStats stats = Metapb.PartitionStats.newBuilder()
                                                           .addAllShard(shardList).build();
        List<Metapb.Shard> shardList2 = new ArrayList<>(stats.getShardList());
        Collections.shuffle(shardList2);
        shardList2.forEach(shard -> {
            System.out.println(shard.getStoreId());
        });


    }
}
