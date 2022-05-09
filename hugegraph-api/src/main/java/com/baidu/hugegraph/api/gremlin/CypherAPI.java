package com.baidu.hugegraph.api.gremlin;

import java.util.Collections;

import org.opencypher.gremlin.translation.TranslationFacade;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

@Path("graphs/{graph}/cypher")
@Singleton
public class CypherAPI extends GremlinQueryAPI {

    private static final Logger LOG = Log.logger(CypherAPI.class);


    @GET
    @Timed
    @CompressInterceptor.Compress(buffer = (1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response query(@PathParam("graph") String graph,
                          @Context HttpHeaders headers,
                          @QueryParam("cypher") String cypher) {
        LOG.debug("Graph [{}] query by cypher: {}", graph, cypher);

        return this.queryByCypher(graph, headers, cypher);
    }

    @POST
    @Timed
    @CompressInterceptor.Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@PathParam("graph") String graph,
                         @Context HttpHeaders headers,
                         String cypher) {
        LOG.debug("Graph [{}] query by cypher: {}", graph, cypher);
        return this.queryByCypher(graph, headers, cypher);
    }

    private Response queryByCypher(String graph,
                                   HttpHeaders headers,
                                   String cypher) {
        E.checkArgument(cypher != null && !cypher.isEmpty(),
                        "The cypher parameter can't be null or empty");

        String gremlin = this.translateCpyher2Gremlin(graph, cypher);
        LOG.debug("translated gremlin is {}", gremlin);

        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.put("gremlin", Collections.singletonList(gremlin));
        Response response = this.client().doGetRequest(auth, params);
        return transformResponseIfNeeded(response);
    }

    private String translateCpyher2Gremlin(String graph, String cypher) {
        TranslationFacade translator = new TranslationFacade();
        String gremlin = translator.toGremlinGroovy(cypher);
        gremlin = this.buildQueryableGremlin(graph, gremlin);
        return gremlin;
    }

    private String buildQueryableGremlin(String graph, String gremlin) {
        gremlin = "g = " + graph + ".traversal()\n" + gremlin;

        // hg not support single
        gremlin = gremlin.replace(".property(single,", ".property(");

        return gremlin;
    }
}
