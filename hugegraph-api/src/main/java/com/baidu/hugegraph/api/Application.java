package com.baidu.hugegraph.api;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;

import com.baidu.hugegraph.core.GraphManager;

@ApplicationPath("/")
public class Application extends ResourceConfig {

    public Application() {
        packages("com.baidu.hugegraph.api");

        // TODO: read from conf
        Map<String, String> graphConfs = new HashMap<>();
        graphConfs.put("hugegraph",
                "../hugegraph-dist/src/assembly/static/conf/hugegraph.properties");
        register(new GraphManagerFactory(graphConfs));
    }

    static class GraphManagerFactory extends AbstractBinder
            implements Factory<GraphManager> {

        private GraphManager manager = null;

        public GraphManagerFactory(final Map<String, String> graphConfs) {
            this.manager = new GraphManager(graphConfs);
        }

        @Override
        protected void configure() {
            bindFactory(this).to(GraphManager.class).in(RequestScoped.class);
        }

        @Override
        public GraphManager provide() {
            return this.manager;
        }

        @Override
        public void dispose(GraphManager manager) {
            // pass
        }
    }
}
