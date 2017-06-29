package com.baidu.hugegraph.api.schema;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/5/18.
 */

@Path("graphs/{graph}/schema/vertexlabels")
@Singleton
public class VertexLabelAPI extends API {
    private static final Logger logger = LoggerFactory.getLogger(VertexLabelAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertexLabel jsonVertexLabel) {
        logger.debug("Graph [{}] create vertex label: {}", graph,
                jsonVertexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        VertexLabel vertexLabel = jsonVertexLabel.convert2VertexLabel();
        g.schema().create(vertexLabel);

        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String append(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertexLabel jsonVertexLabel) {
        logger.debug("Graph [{}] append vertex label: {}", graph,
                jsonVertexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        VertexLabel vertexLabel = jsonVertexLabel.convert2VertexLabel();
        g.schema().append(vertexLabel);

        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        logger.debug("Graph [{}] get vertex labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<VertexLabel> vertexLabels = g.schemaTransaction().getVertexLabels();

        return manager.serializer(g).writeVertexLabels(vertexLabels);
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        logger.debug("Graph [{}] get vertex label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        VertexLabel vertexLabel = g.schemaTransaction().getVertexLabel(name);
        checkExists(vertexLabel, name);
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        logger.debug("Graph [{}] remove vertex label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removeVertexLabel(name);
    }

    private static class JsonVertexLabel {

        public String name;
        public String[] primaryKeys;
        public String[] indexNames;
        public String[] properties;
        public boolean checkExist;

        @Override
        public String toString() {
            return String.format("{name=%s, primaryKeys=%s, "
                            + "indexNames=%s, properties=%s}",
                    this.name,
                    this.primaryKeys,
                    this.indexNames,
                    this.properties);
        }

        public VertexLabel convert2VertexLabel() {
            HugeVertexLabel vertexLabel = new HugeVertexLabel(this.name);

            vertexLabel.primaryKeys(this.primaryKeys);
            vertexLabel.indexNames(this.indexNames);
            vertexLabel.properties(this.properties);
            vertexLabel.checkExist(this.checkExist);
            return vertexLabel;
        }
    }
}
