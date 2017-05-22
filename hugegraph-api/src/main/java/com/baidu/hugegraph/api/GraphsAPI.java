package com.baidu.hugegraph.api;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.baidu.hugegraph.core.GraphManager;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Object list(@Context GraphManager manager) {
        return ImmutableMap.of("graphs", manager.graphs().keySet());
    }
}
