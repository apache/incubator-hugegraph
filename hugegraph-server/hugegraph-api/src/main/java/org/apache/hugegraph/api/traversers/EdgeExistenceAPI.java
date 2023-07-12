package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.EdgeExistenceTraverser;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;

@Path("graphs/{graph}/traversers/edgeexistence")
@Singleton
@Tag(name = "EdgeExistenceAPI")
public class EdgeExistenceAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(EdgeExistenceAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("edgelabel") String edgeLabel,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_LIMIT) long limit) {
        LOG.debug("Graph [{}] get edgeexistence with " +
                "source '{}', target '{}', edgeLabel '{}' and limit '{}'",
            graph, source, target, edgeLabel, limit);

        E.checkArgumentNotNull(source, "The source can't be null");
        E.checkArgumentNotNull(target, "The target can't be null");

        Id sourceId = HugeVertex.getIdValue(source);
        Id targetId = HugeVertex.getIdValue(target);
        HugeGraph hugeGraph = graph(manager, graph);
        EdgeExistenceTraverser traverser = new EdgeExistenceTraverser(hugeGraph);

        Iterator<Edge> edges = traverser.queryEdgeExistence(sourceId, targetId, edgeLabel, limit);

        List<Edge> all = Lists.newArrayList(edges);
        return manager.serializer(hugeGraph).writeList("edges", all);
    }
}
