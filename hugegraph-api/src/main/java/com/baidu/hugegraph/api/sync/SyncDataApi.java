package com.baidu.hugegraph.api.sync;

import javax.inject.Singleton;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.kafka.KafkaSyncConsumer;
import com.baidu.hugegraph.kafka.KafkaSyncConsumerBuilder;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;

import org.slf4j.Logger;

@Path("graphspace/{graphspace}/graphs/sync")
@Singleton
public class SyncDataApi extends API {
    private static final Logger LOG = Log.logger(RestServer.class);


    @PUT
    @Path("{graph}")
    public Object sync(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph
    ) {

        KafkaSyncConsumerBuilder.setGraphManager(manager);


        return null;
    }
    
}
