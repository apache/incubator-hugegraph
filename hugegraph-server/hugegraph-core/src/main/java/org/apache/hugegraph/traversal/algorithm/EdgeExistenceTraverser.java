package org.apache.hugegraph.traversal.algorithm;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EdgeExistenceTraverser extends HugeTraverser {
    public EdgeExistenceTraverser(HugeGraph graph) {
        super(graph);
    }

    public Iterator<Edge> queryEdgeExistence(
        Id sourceId, Id targetId,
        String label, long limit) {
        if (label == null || label.isEmpty()) {
            return queryByNeighbor(sourceId, targetId, limit);
        }
        Id edgeLabelId = getEdgeLabelId(label);
        EdgeLabel edgeLabel = EdgeLabel.undefined(graph(), edgeLabelId);
        List<Id> sortKeys = edgeLabel.sortKeys();

        ConditionQuery conditionQuery = new ConditionQuery(HugeType.EDGE);
        conditionQuery.eq(HugeKeys.OWNER_VERTEX, sourceId);
        conditionQuery.eq(HugeKeys.OTHER_VERTEX, targetId);
        conditionQuery.eq(HugeKeys.LABEL, edgeLabelId);
        conditionQuery.eq(HugeKeys.DIRECTION, Directions.OUT);
        if (sortKeys.size() == 0) {
            conditionQuery.eq(HugeKeys.SORT_VALUES, "");
        } else {
            conditionQuery.eq(HugeKeys.SORT_VALUES, sortKeys);
        }
        return graph().edges(conditionQuery);
    }

    private Iterator<Edge> queryByNeighbor(Id sourceId, Id targetId, long limit) {
        Iterator<Edge> edges = this.edgesOfVertex(sourceId, Directions.OUT, (Id) null, limit);
        List<Edge> res = new ArrayList<>();
        String target = targetId.toString();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            String outVertexId = edge.inVertex().id().toString();
            if (!target.equals(outVertexId)) continue;
            res.add(edge);
        }
        return res.iterator();
    }
}
