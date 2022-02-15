/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.api.space;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessionsImpl;

@Path("hstore")
@Singleton
public class HStoreAPI extends API {
    private static final Logger LOG = Log.logger(RestServer.class);
    private PDClient client;

    protected synchronized PDClient client() {
        if (this.client != null) {
            return this.client;
        }
        this.client = HstoreSessionsImpl.getDefaultPdClient();

        E.checkArgument(client != null, "Get pd client error, The hstore api " +
                "is not enable.");

        return this.client;
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@QueryParam("offlineExcluded")
                       @DefaultValue("true") boolean offlineExcluded) {

        LOG.debug("List all hstore node");

        List<Long> nodes = new ArrayList<Long>();

        List<Metapb.Store> stores = null;
        try {
            stores = client().getStoreStatus(offlineExcluded);
        } catch (PDException e) {
            throw new HugeException("Get hstore nodes error", e);
        }

        for (Metapb.Store store: stores) {
            // 节点id
            long id = store.getId();
            nodes.add(id);
        }

        return ImmutableMap.of("nodes", nodes);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("id") long id) {

        Metapb.Store store = null;
        try {
            store = client().getStore(id);
        } catch (PDException e) {
            throw new HugeException("Get hstore node by id error", e);
        }

        E.checkArgument(store != null, "Get store by (%d) not exist", id);

        Metapb.StoreStats stats = store.getStats();
        // 总空间大小
        long capacity = stats.getCapacity();
        // 使用大小
        long used = stats.getUsedSize();
        // 状态
        Metapb.StoreState state = store.getState();
        // 分片数量

        List<Metapb.Partition> partitions = null;

        try {
            partitions = client().getPartitionsByStore(id);
        } catch (PDException e) {
            throw new HugeException("Get partitions by node id error", e);
        }

        List<Map> partitionInfos = new ArrayList<>();
        for(Metapb.Partition partition: partitions) {
            int pid = partition.getId();
            String graphName = partition.getGraphName();
            partitionInfos.add(ImmutableMap.of("id", pid,
                                               "graph_name", graphName));
        }

        HashMap<String, Object> storeInfo = new HashMap<String, Object>();
        storeInfo.put("id", id);
        storeInfo.put("capacity", capacity);
        storeInfo.put("used", used);
        storeInfo.put("state", state.name());
        storeInfo.put("partitions", partitionInfos);

        return storeInfo;
    }
}
