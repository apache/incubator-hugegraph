package com.baidu.hugegraph.api;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.HugeServer;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Object list(@Context GraphManager manager) {
        return ImmutableMap.of("graphs", manager.graphs().keySet());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        logger.debug("Graphs [{}] get graph by name '{}'", name);

        HugeGraph g = (HugeGraph) graph(manager, name);
        return ImmutableMap.of("name", g.name());
    }

}
