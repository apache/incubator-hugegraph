package com.baidu.hugegraph.api.profile;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.codahale.metrics.annotation.Timed;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
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

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;

@Path("graphspaces/{graphspace}/configs")
@Singleton
public class ConfigAPI extends API {


    /**
     * Available rest fields
     */
    private static final Set<String> REST_FIELDS
            = new HashSet<>(Arrays.asList(
                "restserver.url",
                "server.start_ignore_single_graph_error",
                "batch.max_write_ratio",
                "batch.max_write_threads",
                "batch.max_vertices_per_batch",
                "batch.max_edges_per_batch",
                "server.k8s_url",
                "server.k8s_use_ca",
                "server.k8s_ca",
                "server.k8s_client_ca",
                "server.k8s_client_key",
                "server.k8s_oltp_image",
                "k8s.internal_algorithm_image_url",
                "k8s.internal_algorithm",
                "k8s.algorithms"
            ));


    @GET
    @Timed
    @Path("rest")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String getRestConfig(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace) {
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace));
    }

    @GET
    @Timed
    @Path("rest/{servicename}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String getRestConfig(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("servicename") String serviceName) {
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       serviceName));
    }

    @GET
    @Timed
    @Path("rest/config-fields")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String getRestConfigFields(@Context GraphManager manager) {

        return manager.serializer().writeList("fields", REST_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @POST
    @Timed
    @Path("rest")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String restAll(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          Map<String, Object> extendProperties) {
        String serviceName = String.valueOf(extendProperties.get("name"));

        Map<String, Object> properties = (Map<String, Object>)extendProperties.get("config");

        // this.validateFields(properties);

        Map<String, Object> result = manager.restProperties(graphSpace, serviceName, properties);
        return manager.serializer().writeMap(result);
        
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
        // this.validateFields(properties);
        checkRestUpdate(properties);
        // manager.createServiceRestConfig(graphSpace, serviceName, properties);
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       // serviceName,
                                                       properties));
    }

    @PUT
    @Timed
    @Path("rest/{servicename}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String rest(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("servicename") String serviceName,
                       Map<String, Object> properties) {
        // this.validateFields(properties);
        checkRestUpdate(properties);
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       serviceName,
                                                       properties));
    }

    @DELETE
    @Timed
    @Path("rest/{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public void rest(@Context GraphManager manager,
                     @PathParam("graphspace") String graphSpace,
                     @PathParam("key") String key) {
        manager.deleteRestProperties(graphSpace, key);
    }

    @DELETE
    @Timed
    @Path("rest/{servicename}/{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public void rest(@Context GraphManager manager,
                     @PathParam("graphspace") String graphSpace,
                     @PathParam("servicename") String serviceName,
                     @PathParam("key") String key) {
        manager.deleteRestProperties(graphSpace, serviceName, key);
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

    
    /**
     * Validate the keys of properties. Not used currently
     * @param properties
     */
    @SuppressWarnings("unused")
    private void validateFields(Map<String, Object> properties) {
        if (null == properties) {
            throw new BadRequestException("Config is null while setting rest config");
        }
        properties.keySet().forEach((key) -> {
            if (!REST_FIELDS.contains(key)) {
                throw new BadRequestException("Invalid filed [" + key + "] while setting rest config");
            }
        });
    }

    private void checkRestUpdate(Map<String, Object> properties) {

    }
}
