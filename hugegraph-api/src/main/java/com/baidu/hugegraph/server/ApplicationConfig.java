package com.baidu.hugegraph.server;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;

import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.baidu.hugegraph.core.GraphManager;
import com.google.common.base.Preconditions;

@ApplicationPath("/")
public class ApplicationConfig extends ResourceConfig {

    public ApplicationConfig(HugeConfiguration conf) {
        packages("com.baidu.hugegraph.api");

        // json support
        register(org.glassfish.jersey.jackson.JacksonFeature.class);

        // register Configuration to context
        register(new ConfFactory(conf));

        // TODO: read from conf
        Map<String, String> graphConfs = new HashMap<>();
        graphConfs.put("hugegraph",
                "/etc/hugegraph/hugegraph.properties");
        register(new GraphManagerFactory(graphConfs));
    }

    static class ConfFactory extends AbstractBinder
            implements Factory<HugeConfiguration> {

        private HugeConfiguration conf = null;

        public ConfFactory(HugeConfiguration conf) {
            Preconditions.checkNotNull(conf, "Configuration is null");
            this.conf = conf;
        }

        @Override
        protected void configure() {
            bindFactory(this).to(HugeConfiguration.class).in(RequestScoped.class);
        }

        @Override
        public HugeConfiguration provide() {
            return this.conf;
        }

        @Override
        public void dispose(HugeConfiguration conf) {
            // pass
        }
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
