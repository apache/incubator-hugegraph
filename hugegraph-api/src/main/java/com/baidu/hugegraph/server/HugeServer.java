package com.baidu.hugegraph.server;

import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.api.Application;
import com.google.common.base.Preconditions;

public class HugeServer {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    private HttpServer httpServer = null;

    public void start() throws IllegalArgumentException, IOException {
        // TODO: read from conf
        URI uri = UriBuilder.fromUri("http://127.0.0.1").port(8080).build();

        ResourceConfig rc = new Application();

        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, rc);
        this.httpServer.start();
    }

    public void stop() {
        Preconditions.checkNotNull(this.httpServer);
        this.httpServer.stop();
    }

    public static void initEnv(String[] args) {
        // pass
    }

    public static HugeServer start(String[] args) {
        logger.info("HugeServer starting...");
        // HugeServer.loadConf(args);
        HugeServer.initEnv(args);

        HugeServer server = new HugeServer();
        try {
            server.start();
            logger.info("HugeServer started");
        } catch (IOException e) {
            logger.error("Failed to start HugeServer", e);
        }

        return server;
    }

    public static void main(String[] args) throws Exception {
        HugeServer.start(args);
        Thread.currentThread().join();
        logger.info("HugeServer stopped");
    }
}
