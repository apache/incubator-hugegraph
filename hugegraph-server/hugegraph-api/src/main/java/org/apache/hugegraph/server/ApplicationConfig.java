/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.server;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.filter.RedirectFilterDynamicFeature;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.WorkLoad;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.version.CoreVersion;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey3.InstrumentedResourceMethodApplicationListener;

import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import jakarta.ws.rs.ApplicationPath;

@SecurityScheme(
    name = "basic",
    type = SecuritySchemeType.HTTP,
    scheme = "basic"
)
@SecurityScheme(
    name = "bearer",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer"
)
@ApplicationPath("/")
@OpenAPIDefinition(
    info = @Info(
        title = "HugeGraph RESTful API",
        version = CoreVersion.DEFAULT_VERSION,
        description = "All management API for HugeGraph",
        contact = @Contact(url = "https://github.com/apache/hugegraph", name = "HugeGraph")
    ),
    security = {@SecurityRequirement(name = "basic"), @SecurityRequirement(name = "bearer")}
)
public class ApplicationConfig extends ResourceConfig {

    public ApplicationConfig(HugeConfig conf, EventHub hub) {
        packages("org.apache.hugegraph.api");

        // Register Jackson to support json
        register(org.glassfish.jersey.jackson.JacksonFeature.class);

        // Register to use the jsr250 annotations @RolesAllowed
        register(RolesAllowedDynamicFeature.class);

        register(RedirectFilterDynamicFeature.class);

        // Register HugeConfig to context
        register(new ConfFactory(conf));

        // Register GraphManager to context
        register(new GraphManagerFactory(conf, hub));

        // Register WorkLoad to context
        register(new WorkLoadFactory());

        // Let @Metric annotations work
        MetricRegistry registry = MetricManager.INSTANCE.getRegistry();
        register(new InstrumentedResourceMethodApplicationListener(registry));

        // Register OpenApi file to support display on swagger-ui
        register(OpenApiResource.class);
    }

    private class ConfFactory extends AbstractBinder implements Factory<HugeConfig> {

        private final HugeConfig conf;

        public ConfFactory(HugeConfig conf) {
            E.checkNotNull(conf, "configuration");
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

    private class GraphManagerFactory extends AbstractBinder implements Factory<GraphManager> {

        private GraphManager manager = null;

        public GraphManagerFactory(HugeConfig conf, EventHub hub) {
            register(new ApplicationEventListener() {
                private final ApplicationEvent.Type eventInited =
                              ApplicationEvent.Type.INITIALIZATION_FINISHED;
                private final ApplicationEvent.Type eventDestroyed =
                              ApplicationEvent.Type.DESTROY_FINISHED;

                @Override
                public void onEvent(ApplicationEvent event) {
                    if (event.getType() == this.eventInited) {
                        GraphManager manager = new GraphManager(conf, hub);
                        try {
                            manager.init();
                        } catch (Throwable e) {
                            manager.close();
                            throw e;
                        }
                        GraphManagerFactory.this.manager = manager;
                    } else if (event.getType() == this.eventDestroyed) {
                        if (GraphManagerFactory.this.manager != null) {
                            GraphManagerFactory.this.manager.close();
                        }
                    }
                }

                @Override
                public RequestEventListener onRequest(RequestEvent event) {
                    return null;
                }
            });
        }

        @Override
        protected void configure() {
            bindFactory(this).to(GraphManager.class).in(RequestScoped.class);
        }

        @Override
        public GraphManager provide() {
            if (this.manager == null) {
                String message = "Please wait for the server to initialize";
                throw new MultiException(new HugeException(message), false);
            }
            return this.manager;
        }

        @Override
        public void dispose(GraphManager manager) {
            // pass
        }
    }

    private class WorkLoadFactory extends AbstractBinder implements Factory<WorkLoad> {

        private final WorkLoad load;

        public WorkLoadFactory() {
            this.load = new WorkLoad();
        }

        @Override
        public WorkLoad provide() {
            return this.load;
        }

        @Override
        public void dispose(WorkLoad workLoad) {
            // pass
        }

        @Override
        protected void configure() {
            bindFactory(this).to(WorkLoad.class).in(RequestScoped.class);
        }
    }
}
