package com.baidu.hugegraph.api.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.HugeServer;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/edges")
@Singleton
public class EdgeAPI extends API {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    private static final int MAX_EDGES = 500;

    @POST
    @Status(Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         CreateEdge edge) {
        logger.debug("Graph [{}] create edge: {}", graph, edge);

        Graph g = graph(manager, graph);

        Vertex srcVertex = g.traversal().V(edge.source).next();
        Vertex tgtVertex = g.traversal().V(edge.target).next();
        Edge e = srcVertex.addEdge(edge.label, tgtVertex, edge.properties());

        return manager.serializer(g).writeEdge(e);
    }

    @POST
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         List<CreateEdge> edges) {
        if (edges.size() > MAX_EDGES) {
            throw new HugeException("Too many counts of edges for one time "
                    + "post, the maximum number is '%s'", MAX_EDGES);
        }

        logger.debug("Graph [{}] create edges: {}", graph, edges);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<String> ids = new ArrayList<>(edges.size());
        g.tx().open();
        try {
            for (CreateEdge edge : edges) {
                Vertex srcVertex = this.newVertex(g, edge.source,
                        edge.sourceLabel);
                Vertex tgtVertex = this.newVertex(g, edge.target,
                        edge.targetLabel);
                ids.add(srcVertex.addEdge(edge.label, tgtVertex,
                        edge.properties()).id().toString());
            }
            g.tx().commit();
        } catch (Exception e) {
            g.tx().rollback();
            throw new HugeException("Failed to add edges", e);
        } finally {
            g.tx().close();
        }
        return ids;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @DefaultValue("100") @QueryParam("limit") long limit) {
        logger.debug("Graph [{}] get vertices", graph);

        Graph g = graph(manager, graph);
        List<Edge> rs = g.traversal().E().limit(limit).toList();
        return manager.serializer(g).writeEdges(rs);
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        logger.debug("Graph [{}] get vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        return manager.serializer(g).writeEdge(g.edges(id).next());
    }

    @DELETE
    @Path("{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        logger.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        // TODO: add removeEdge(id) to improve
        g.edges(id).next().remove();
    }

    private Vertex newVertex(HugeGraph graph, String id, String label) {
        VertexLabel vertexLabel = graph.schemaTransaction()
                .getVertexLabel(label);
        E.checkNotNull(vertexLabel, "Not found this vertex label '%s'", label);
        Vertex vertex = new HugeVertex(graph.graphTransaction(),
                HugeElement.getIdValue(T.id, id), vertexLabel);
        return vertex;
    }

    static class CreateEdge {

        public String source;
        @JsonProperty("outVLabel")
        public String sourceLabel;
        public String label;
        public String target;
        @JsonProperty("inVLabel")
        public String targetLabel;
        public Map<String, Object> properties;

        public Object[] properties() {
            return API.properties(this.properties);
        }

        @Override
        public String toString() {
            return String.format("{label=%s, source-vertex=%s, "
                    + "source-vertex-label=%s, target-vertex=%s, "
                    + "target-vertex-label=%s, properties=%s}",
                    this.label, this.source, this.sourceLabel,
                    this.target, this.targetLabel, this.properties);
        }
    }
}
