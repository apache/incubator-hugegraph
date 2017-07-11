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
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.schema.PropertyKey;

/**
 * Created by liningrui on 2017/5/18.
 */

@Path("graphs/{graph}/schema/propertykeys")
@Singleton
public class PropertyKeyAPI extends API {

    private static final Logger logger =
            LoggerFactory.getLogger(PropertyKeyAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonPropertyKey jsonPropertyKey) {
        logger.debug("Graph [{}] create property key: {}",
                     graph, jsonPropertyKey);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        PropertyKey propertyKey = jsonPropertyKey.convert2PropertyKey();
        g.schema().create(propertyKey);

        return manager.serializer(g).writePropertyKey(propertyKey);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        logger.debug("Graph [{}] get property keys", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<PropertyKey> propKeys = g.schemaTransaction().getPropertyKeys();

        return manager.serializer(g).writePropertyKeys(propKeys);
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        logger.debug("Graph [{}] get property key by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        PropertyKey propertyKey = g.schemaTransaction().getPropertyKey(name);
        checkExists(propertyKey, name);
        return manager.serializer(g).writePropertyKey(propertyKey);
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        logger.debug("Graph [{}] remove property key by name '{}'",
                     graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removePropertyKey(name);
    }

    static class JsonPropertyKey {

        public String name;
        public Cardinality cardinality;
        public DataType dataType;
        public String[] properties;
        public boolean checkExist;

        @Override
        public String toString() {
            return String.format("JsonPropertyKey{name=%s, cardinality=%s, " +
                                 "dataType=%s, properties=%s}",
                                 this.name, this.cardinality,
                                 this.dataType, this.properties);
        }

        public PropertyKey convert2PropertyKey() {
            HugePropertyKey propertyKey = new HugePropertyKey(this.name);
            propertyKey.cardinality(this.cardinality);
            propertyKey.dataType(this.dataType);
            propertyKey.properties(this.properties);
            propertyKey.checkExist(this.checkExist);
            return propertyKey;
        }
    }
}
