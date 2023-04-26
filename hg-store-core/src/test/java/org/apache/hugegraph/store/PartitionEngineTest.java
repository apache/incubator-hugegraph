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

package org.apache.hugegraph.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.meta.Partition;
import org.junit.Assert;

import com.alipay.sofa.jraft.option.RpcOptions;

public class PartitionEngineTest {

    // @Test
    public void test() {
        String[] peers = new String[]{"1", "2", "6"};
        String[] learners = new String[]{"3", "7"};
        List<String> oldPeers1 = Arrays.asList("1", "2", "3");
        List<String> oldLearner1 = Arrays.asList("4", "5");


        List<String> oldPeers =
                oldPeers1.stream().map(peer -> peer).collect(Collectors.toList());
        oldPeers.addAll(
                oldLearner1.stream().map(peer -> peer).collect(Collectors.toList()));

        List<String> addedNodes = Arrays.stream(peers).filter(peer ->
                                                                      !oldPeers.stream()
                                                                               .map(e -> e)
                                                                               .collect(
                                                                                       Collectors.toSet())
                                                                               .contains(peer))
                                        .collect(Collectors.toList());
        addedNodes.addAll(Arrays.stream(learners).filter(peer ->
                                                                 !oldPeers.stream()
                                                                          .map(e -> e)
                                                                          .collect(
                                                                                  Collectors.toSet())
                                                                          .contains(peer))
                                .collect(Collectors.toList()));

        List<String> removedNodes = oldPeers.stream().filter(peer ->
                                                                     !Arrays.stream(peers)
                                                                            .map(e -> e).collect(
                                                                                     Collectors.toSet())
                                                                            .contains(peer))
                                            .collect(Collectors.toList());
        removedNodes = removedNodes.stream().filter(peer ->
                                                            !Arrays.stream(learners).map(e -> e)
                                                                   .collect(Collectors.toSet())
                                                                   .contains(peer))
                                   .collect(Collectors.toList());
        List<String> mixedPeer = oldPeers1.stream().filter(peer ->
                                                                   Arrays.stream(learners)
                                                                         .map(e -> e).collect(
                                                                                 Collectors.toSet())
                                                                         .contains(peer))
                                          .collect(Collectors.toList());

        // 新增 6、7
        Assert.assertEquals(2, addedNodes.size());
        addedNodes.clear();
        addedNodes.addAll(Arrays.asList(peers));
        addedNodes.addAll(Arrays.asList(learners));
        addedNodes.removeAll(oldPeers);
        Assert.assertEquals(2, addedNodes.size());

        addedNodes.forEach(s -> System.out.print(s + " "));
        System.out.println();
        // 删除 4，5
        Assert.assertEquals(2, removedNodes.size());

        removedNodes.clear();
        removedNodes.addAll(oldPeers);
        removedNodes.removeAll(Arrays.asList(peers));
        removedNodes.removeAll(Arrays.asList(learners));
        Assert.assertEquals(2, removedNodes.size());
        removedNodes.forEach(s -> System.out.print(s + " "));
        System.out.println();
        // 交集 5
        Assert.assertEquals(1, mixedPeer.size());
        oldPeers1.removeAll(Arrays.asList(learners));
        Assert.assertEquals(1, oldPeers1.size());
        mixedPeer.forEach(s -> System.out.print(s + " "));

    }


    // @Test
    public void testPartition() {
        Partition p = new Partition();
        p.setId(1);
        List<Metapb.Shard> shards = new ArrayList<>();
        shards.add(Metapb.Shard.newBuilder().build());
        // p.setShardsList(shards);

        Partition p2 = p.clone();
        p.setId(2);
        Assert.assertNotEquals(p2.getId(), p.getId());

    }

    // @Test
    public void testUpdateShardsList() {
        List<Metapb.Shard> curShards = new ArrayList<>();
        curShards.add(Metapb.Shard.newBuilder().setStoreId(1001).setRole(Metapb.ShardRole.Follower)
                                  .build());
        curShards.add(Metapb.Shard.newBuilder().setStoreId(1002).setRole(Metapb.ShardRole.Leader)
                                  .build());
        curShards.add(Metapb.Shard.newBuilder().setStoreId(1003).setRole(Metapb.ShardRole.Follower)
                                  .build());

        List<Metapb.Shard> reqShards = new ArrayList<>();
        reqShards.add(Metapb.Shard.newBuilder().setStoreId(1001).setRole(Metapb.ShardRole.Leader)
                                  .build());
        reqShards.add(Metapb.Shard.newBuilder().setStoreId(1002).setRole(Metapb.ShardRole.Leader)
                                  .build());
        reqShards.add(Metapb.Shard.newBuilder().setStoreId(1004).setRole(Metapb.ShardRole.Leader)
                                  .build());


        long leaderStoreId = 0;
        for (Metapb.Shard shard : curShards) {
            if (shard.getRole() == Metapb.ShardRole.Leader) {
                leaderStoreId = shard.getStoreId();
                break;
            }
        }

        // remove
        List<Metapb.Shard> shards = curShards.stream().filter(shard ->
                                                                      reqShards.stream()
                                                                               .map(Metapb.Shard::getStoreId)
                                                                               .collect(
                                                                                       Collectors.toSet())
                                                                               .contains(
                                                                                       shard.getStoreId()))
                                             .collect(Collectors.toList());

        // add
        List<Metapb.Shard> addShards = reqShards.stream().filter(shard ->
                                                                         !curShards.stream()
                                                                                   .map(Metapb.Shard::getStoreId)
                                                                                   .collect(
                                                                                           Collectors.toSet())
                                                                                   .contains(
                                                                                           shard.getStoreId()))
                                                .collect(Collectors.toList());
        shards.addAll(addShards);

        // change leader
        for (Metapb.Shard shard : shards) {
            if (shard.getStoreId() == leaderStoreId) {
                shard.toBuilder().setRole(Metapb.ShardRole.Leader).build();
            } else {
                shard.toBuilder().setRole(Metapb.ShardRole.Follower).build();
            }
        }

    }

    // @Test
    public void testPriority() {
        List<String> oldPeers = new ArrayList<>();
        oldPeers.add("127.0.0.1:8001::100");
        oldPeers.add("127.0.0.1:8002::75");
        oldPeers.add("127.0.0.1:8003::50");

        List<String> peers = new ArrayList<>();
        peers.add("127.0.0.1:8001");
        peers.add("127.0.0.1:8003::60");
        peers.add("127.0.0.1:8004");

        List<String> priPeers = new ArrayList<>();
        for (String peer : peers) {
            if (peer.contains("::")) {
                priPeers.add(peer);
                System.out.println(peer);
            } else {
                boolean find = false;
                for (String oldPeer : oldPeers) {
                    if (oldPeer.contains(peer + "::")) {
                        find = true;
                        priPeers.add(oldPeer);
                        System.out.println(oldPeer);
                        break;
                    }
                }
                if (!find) {
                    priPeers.add(peer);
                    System.out.println(peer);
                }
            }
        }


    }

    //    @Test
    public void testRpcCall() throws ExecutionException, InterruptedException {
        HgCmdClient client = new HgCmdClient();
        client.init(new RpcOptions(), null);
        ArrayList<Partition> ps = new ArrayList<>();
        Metapb.Partition p = Metapb.Partition.newBuilder()
                                             .setGraphName("OK")
                                             .setId(1)
                                             // .addShards(Metapb.Shard.newBuilder().setStoreId
                                             // (1).build())
                                             .build();
        ps.add(new Partition(p));
        client.createRaftNode("127.0.0.1:8511", ps, status -> {
            System.out.println(status);
        }).get();
    }
}
