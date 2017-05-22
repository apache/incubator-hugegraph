package com.baidu.hugegraph.api.schema;

import java.util.List;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.HugeIndexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.IndexLabel;

/**
 * Created by liningrui on 2017/5/22.
 */

@Path("graphs/{graph}/schema/indexlabels")
@Singleton
public class IndexLabelAPI extends API {
    private static final Logger logger = LoggerFactory.getLogger(VertexLabelAPI.class);

    @POST
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         IndexLabelAPI.CreateIndexLabel createIndexLabel) {
        logger.debug("Graph [{}] create index label: {}", graph, createIndexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        IndexLabel indexLabel = createIndexLabel.convert2IndexLabel();
        g.schemaTransaction().addIndexLabel(indexLabel);

        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        logger.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<HugeIndexLabel> indexLabels = g.schemaTransaction().getIndexLabels();

        return manager.serializer(g).writeIndexlabels(indexLabels);
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        logger.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        IndexLabel indexLabel = g.schemaTransaction().getIndexLabel(name);
        checkExists(indexLabel, name);
        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        logger.debug("Graph [{}] remove index label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removeIndexLabel(name);
    }

    private static class CreateIndexLabel {

        public String name;
        public HugeType baseType;
        public String baseValue;
        public IndexType indexType;
        public String[] fields;


        @Override
        public String toString() {
            return String.format("{name=%s, baseType=%s, baseValue=%s"
                            + "indexType=%s, fields=%s}",
                    this.name,
                    this.baseType,
                    this.baseValue,
                    this.indexType,
                    this.fields);
        }

        public IndexLabel convert2IndexLabel() {
            HugeIndexLabel indexLabel = new HugeIndexLabel(this.name);
            indexLabel.baseType(this.baseType);
            indexLabel.baseValue(this.baseValue);
            indexLabel.indexType(this.indexType);
            indexLabel.indexFields(this.fields);

            return indexLabel;
        }
    }
}
