package com.baidu.hugegraph.api.schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.schema.EdgeLabel;

/**
 * Created by liningrui on 2017/5/18.
 */

@Path("graphs/{graph}/schema/edgelabels")
@Singleton
public class EdgeLabelAPI extends API {
    private static final Logger logger = LoggerFactory.getLogger(EdgeLabelAPI.class);

    @POST
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         CreateEdgeLabel createEdgeLabel) {
        logger.debug("Graph [{}] create edge label: {}", graph, createEdgeLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        EdgeLabel edgeLabel = createEdgeLabel.convert2EdgeLabel();
        g.schemaTransaction().addEdgeLabel(edgeLabel);

        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        logger.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<HugeEdgeLabel> edgeLabels = g.schemaTransaction().getEdgeLabels();

        return manager.serializer(g).writeEdgeLabels(edgeLabels);
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        logger.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        EdgeLabel edgeLabel = g.schemaTransaction().getEdgeLabel(name);
        checkExists(edgeLabel, name);
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        logger.debug("Graph [{}] remove edge label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removeEdgeLabel(name);
    }

    private static class CreateEdgeLabel {

        public String name;
        public Frequency frequency;
        public EdgeLink[] links;
        public String[] sortKeys;
        public String[] indexNames;
        public String[] properties;

        @Override
        public String toString() {
            return String.format("{name=%s, frequency=%s, links=%s,"
                            + " sortKeys=%s, indexNames=%s, properties=%s}",
                    this.name, this.frequency, this.links,
                    this.sortKeys, this.indexNames, this.properties);
        }

        public EdgeLabel convert2EdgeLabel() {
            HugeEdgeLabel edgeLabel = new HugeEdgeLabel(this.name);

            edgeLabel.frequency(this.frequency);
            edgeLabel.links(links);
            edgeLabel.sortKeys(this.sortKeys);
            edgeLabel.indexNames(this.indexNames);
            edgeLabel.properties(this.properties);

            return edgeLabel;
        }
    }

}
