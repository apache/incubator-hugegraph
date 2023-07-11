package org.apache.hugegraph.api.traversers;

import com.codahale.metrics.annotation.Timed;
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
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


@Path("graphs/{graph}/traversers/edgeexistence")
@Singleton
@Tag(name = "EdgeExistenceAPI")
public class EdgeExistenceAPI extends TraverserAPI {

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(
        @Context GraphManager manager,
        @PathParam("graph") String graph,
        @QueryParam("source") String source,
        @QueryParam("target") String target,
        @QueryParam("edgelabel")
        @DefaultValue("BOTH") String edgeLabel,
        @QueryParam("limit")
        @DefaultValue("1000") long limit) {

        E.checkArgumentNotNull(source, "The source can't be null");
        E.checkArgumentNotNull(target, "The target can't be null");

        Id sourceId = HugeVertex.getIdValue(source);
        Id targetId = HugeVertex.getIdValue(target);
        HugeGraph hugeGraph = graph(manager, graph);
        EdgeExistenceTraverser traverser = new EdgeExistenceTraverser(hugeGraph);

        Iterator<Edge> edges = traverser.queryEdgeExistence(sourceId, targetId, edgeLabel, limit);

        List<Id> all = new ArrayList<>();
        while (edges.hasNext()) {
            all.add((Id) edges.next().id());
        }
        return manager.serializer(hugeGraph).writeList("EdgeIds", all);
    }


}
