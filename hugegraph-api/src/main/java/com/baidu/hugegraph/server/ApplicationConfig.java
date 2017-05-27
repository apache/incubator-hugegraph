package com.baidu.hugegraph.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.core.GraphManager;
import com.google.common.base.Preconditions;

@ApplicationPath("/")
public class ApplicationConfig extends ResourceConfig {

    public ApplicationConfig(HugeConfig conf) {
        packages("com.baidu.hugegraph.api");

        // json support
        register(org.glassfish.jersey.jackson.JacksonFeature.class);

        // register Configuration to context
        register(new ConfFactory(conf));

        register(new GraphManagerFactory(parseGraphs(conf)));
    }

    public Map<String, String> parseGraphs(HugeConfig conf) {
        List<Object> graphs = conf.getList(CoreOptions.GRAPHS.name());
        Map<String, String> graphMap = new HashMap<>();
        for (Object graph : graphs) {
            String[] graphPair = ((String) graph).split(":");
            assert graphPair.length == 2;
            graphMap.put(graphPair[0], graphPair[1]);
        }
        return graphMap;
    }

    static class ConfFactory extends AbstractBinder
            implements Factory<HugeConfig> {

        private HugeConfig conf = null;

        public ConfFactory(HugeConfig conf) {
            Preconditions.checkNotNull(conf, "Configuration is null");
            this.conf = conf;
        }

        @Override
        protected void configure() {
            bindFactory(this).to(HugeConfig.class).in(RequestScoped.class);
        }

        @Override
        public HugeConfig provide() {
            return this.conf;
        }

        @Override
        public void dispose(HugeConfig conf) {
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
