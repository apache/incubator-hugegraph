package com.baidu.hugegraph.server;

import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.google.common.base.Preconditions;

public class HugeServer {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    private HugeConfiguration conf = null;

    private HttpServer httpServer = null;

    public HugeServer(HugeConfiguration conf) {
        this.conf = conf;
    }

    public void start() throws IllegalArgumentException, IOException {
        // TODO: read from conf
        URI uri = UriBuilder.fromUri("http://127.0.0.1").port(8080).build();

        ResourceConfig rc = new ApplicationConfig(this.conf);

        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, rc);
        this.httpServer.start();
    }

    public void stop() {
        Preconditions.checkNotNull(this.httpServer);
        this.httpServer.stop();
    }

    public static HugeServer start(String[] args) {
        logger.info("HugeServer starting...");
        HugeConfiguration conf = HugeServer.loadConf(args);

        HugeServer server = new HugeServer(conf);
        try {
            server.start();
            logger.info("HugeServer started");
        } catch (IOException e) {
            logger.error("Failed to start HugeServer", e);
        }

        return server;
    }

    protected static HugeConfiguration loadConf(String[] args) {
        HugeConfiguration conf = new HugeConfiguration();
        // TODO: parse conf
        return conf;
    }

    public static void main(String[] args) throws Exception {
        HugeServer.start(args);
        Thread.currentThread().join();
        logger.info("HugeServer stopped");
    }
}
