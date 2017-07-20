package com.baidu.hugegraph.dist;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph
        .GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeException;

/**
 * Created by liningrui on 2017/5/10.
 */
public class HugeGremlinServer {

    private static final Logger logger =
            LoggerFactory.getLogger(HugeGremlinServer.class);

    private static final String G_PREFIX = "__g_";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new HugeException("HugeGremlinServer can only accept one " +
                                    "config file.");
        }

        RegisterUtil.registerBackends();

        // Start GremlinServer with inject traversal source
        startWithInjectTraversal(args);
    }

    private static void startWithInjectTraversal(String[] args)
                                                 throws Exception {
        logger.info(GremlinServer.getHeader());
        final String file;
        if (args.length > 0)
            file = args[0];
        else
            file = "conf/gremlin-server.yaml";

        final Settings settings;
        try {
            settings = Settings.read(file);
        } catch (Exception ex) {
            logger.error("Configuration file at {} could not be found or " +
                         "parsed properly. [{}]", file, ex.getMessage());
            return;
        }

        logger.info("Configuring Gremlin Server from {}", file);
        final GremlinServer server = new GremlinServer(settings);

        // Inject customized traversal source
        injectTraversalSource(server);

        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now " +
                         "begin shutdown: {}", t.getMessage());
            server.stop().join();
            return null;
        }).join();
    }

    private static void injectTraversalSource(GremlinServer server) {
        GraphManager graphManager = server.getServerGremlinExecutor()
                                          .getGraphManager();
        for (String graph : graphManager.getGraphNames()) {
            GraphTraversalSource g = graphManager.getGraph(graph).traversal();
            String gName = G_PREFIX + graph;
            if (graphManager.getTraversalSource(gName) != null) {
                throw new HugeException(
                          "Found existed name '%s' in global bindings, " +
                          "it may lead gremlin query error.", gName);
            }
            // Add a traversal source for all graphs with customed rule.
            graphManager.putTraversalSource(gName, g);
        }
    }
}
