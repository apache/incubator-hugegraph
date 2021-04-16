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

package com.baidu.hugegraph.api.raft;

import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/raft")
@Singleton
public class RaftAPI extends API {

    private static final Logger LOG = Log.logger(RaftAPI.class);

    @GET
    @Timed
    @Path("list_peers")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, List<String>> listPeers(@Context GraphManager manager,
                                               @PathParam("graph") String graph,
                                               @QueryParam("group")
                                               @DefaultValue("default")
                                               String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "list_peers");
        List<String> peers = raftManager.listPeers();
        return ImmutableMap.of(raftManager.group(), peers);
    }

    @GET
    @Timed
    @Path("get_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> getLeader(@Context GraphManager manager,
                                         @PathParam("graph") String graph,
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "get_leader");
        String leaderId = raftManager.getLeader();
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("transfer_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> transferLeader(@Context GraphManager manager,
                                              @PathParam("graph") String graph,
                                              @QueryParam("group")
                                              @DefaultValue("default")
                                              String group,
                                              @QueryParam("endpoint")
                                              String endpoint) {
        LOG.debug("Graph [{}] prepare to transfer leader to: {}",
                  graph, endpoint);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "transfer_leader");
        String leaderId = raftManager.transferLeaderTo(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("set_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> setLeader(@Context GraphManager manager,
                                         @PathParam("graph") String graph,
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group,
                                         @QueryParam("endpoint")
                                         String endpoint) {
        LOG.debug("Graph [{}] prepare to set leader to: {}",
                  graph, endpoint);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "set_leader");
        String leaderId = raftManager.setLeader(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("add_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> addPeer(@Context GraphManager manager,
                                       @PathParam("graph") String graph,
                                       @QueryParam("group")
                                       @DefaultValue("default")
                                       String group,
                                       @QueryParam("endpoint")
                                       String endpoint) {
        LOG.debug("Graph [{}] prepare to add peer: {}", graph, endpoint);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "add_peer");
        String peerId = raftManager.addPeer(endpoint);
        return ImmutableMap.of(raftManager.group(), peerId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("remove_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, String> removePeer(@Context GraphManager manager,
                                          @PathParam("graph") String graph,
                                          @QueryParam("group")
                                          @DefaultValue("default")
                                          String group,
                                          @QueryParam("endpoint")
                                          String endpoint) {
        LOG.debug("Graph [{}] prepare to remove peer: {}", graph, endpoint);

        HugeGraph g = graph(manager, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "remove_peer");
        String peerId = raftManager.removePeer(endpoint);
        return ImmutableMap.of(raftManager.group(), peerId);
    }

    private static RaftGroupManager raftGroupManager(HugeGraph graph,
                                                     String group,
                                                     String operation) {
        RaftGroupManager raftManager = graph.raftGroupManager(group);
        if (raftManager == null) {
            throw new HugeException("Allowed %s operation only when " +
                                    "working on raft mode", operation);
        }
        return raftManager;
    }
}
