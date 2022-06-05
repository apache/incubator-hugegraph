package com.baidu.hugegraph.api.gremlin;

import org.opencypher.gremlin.translation.TranslationFacade;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.tags.Tag;
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
import jakarta.ws.rs.core.Response;

@Path("graphs/{graph}/cypher")
@Singleton
@Tag(name = "CypherAPI")
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
        String request = "{" +
                         "\"gremlin\":\"" + gremlin + "\"," +
                         "\"bindings\":{}," +
                         "\"language\":\"gremlin-groovy\"," +
                         "\"aliases\":{\"g\":\"__g_" + graph + "\"}}";

        Response response = this.client().doPostRequest(auth, request);
        return transformResponseIfNeeded(response);
    }

    private String translateCpyher2Gremlin(String graph, String cypher) {
        TranslationFacade translator = new TranslationFacade();
        String gremlin = translator.toGremlinGroovy(cypher);
        gremlin = this.buildQueryableGremlin(graph, gremlin);
        return gremlin;
    }

    private String buildQueryableGremlin(String graph, String gremlin) {
        /*
         * `CREATE (a:person { name : 'test', age: 20) return a`
         * would be translated to:
         * `g.addV('person').as('a').property(single, 'name', 'test') ...`,
         * but hugegraph don't support `.property(single, k, v)`,
         * so we replace it to `.property(k, v)` here
         */
        gremlin = gremlin.replace(".property(single,", ".property(");

        return gremlin;
    }
}
