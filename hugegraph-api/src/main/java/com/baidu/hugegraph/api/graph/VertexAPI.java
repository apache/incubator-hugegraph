package com.baidu.hugegraph.api.graph;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.HugeServer;
import com.google.common.collect.ImmutableList;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends API {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         String keyValues) {
        logger.debug("Graph [{}] create vertex: {}", graph, keyValues);

        Graph g = graph(manager, graph);

        // TODO: improve keyValues parse
        Object[] props = keyValues.split(",");
        if (props[0].equals("T.label")) {
            props = ImmutableList.copyOf(props).toArray();
            props[0] = T.label;
        }

        return manager.serializer(g).writeVertex(g.addVertex(props));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @DefaultValue("100") @QueryParam("limit") long limit) {
        logger.debug("Graph [{}] get vertices", graph);

        Graph g = graph(manager, graph);
        List<Vertex> rs = g.traversal().V().limit(limit).toList();
        return manager.serializer(g).writeVertices(rs);
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        logger.debug("Graph [{}] get vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        return manager.serializer(g).writeVertex(g.vertices(id).next());
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        logger.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        // TODO: add removeVertex(id) to improve
        g.vertices(id).next().remove();
    }
}
