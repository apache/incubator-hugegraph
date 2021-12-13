package com.baidu.hugegraph.api.profile;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.codahale.metrics.annotation.Timed;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Path("graphspaces/{graphspace}/configs")
@Singleton
public class ConfigAPI extends API {

    @GET
    @Timed
    @Path("rest")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String rest(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace) {
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace));
    }

    @PUT
    @Timed
    @Path("rest")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String rest(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          Map<String, Object> properties) {
        checkRestUpdate(properties);
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       properties));
    }

    @DELETE
    @Timed
    @Path("rest/{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String rest(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          @PathParam("key") String key) {
        return manager.serializer()
                      .writeMap(manager.deleteRestProperties(graphSpace, key));
    }

    @GET
    @Timed
    @Path("gremlin")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String gremlinYaml(@Context GraphManager manager,
                             @PathParam("graphspace") String graphSpace) {
        return manager.gremlinYaml(graphSpace);
    }

    @PUT
    @Timed
    @Path("gremlin")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String gremlinYaml(@Context GraphManager manager,
                              @PathParam("graphspace") String graphSpace,
                              String yaml) {
        return manager.gremlinYaml(graphSpace, yaml);
    }

    private void checkRestUpdate(Map<String, Object> properties) {

    }
}
