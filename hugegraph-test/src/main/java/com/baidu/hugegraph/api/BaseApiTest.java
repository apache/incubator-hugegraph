package com.baidu.hugegraph.api;

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;

public class BaseApiTest {

    public static String BASE_URL = "http://127.0.0.1:8080";

    private RestClient client;

    @Before
    public void init() {
        this.client = newClient();
    }

    @After
    public void clear() {
        this.client.close();
    }

    public RestClient client() {
        return this.client;
    }

    public static RestClient newClient() {
        return new RestClient(BASE_URL);
    }

    static class RestClient {

        private Client client;
        private WebTarget target;

        public RestClient(String url) {
            this.client = ClientBuilder.newClient();
            this.target = this.client.target(url);
        }

        public void close() {
            this.client.close();
        }

        public WebTarget target() {
            return this.target;
        }

        public WebTarget target(String url) {
            return this.client.target(url);
        }

        public Response get(String path) {
            return this.target.path(path)
                    .request()
                    .get();
        }

        public Response get(String path, String id) {
            return this.target.path(path).path(id)
                    .request()
                    .get();
        }

        public Response get(String path, Map<String, Object> params) {
            WebTarget t = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                t = t.queryParam(i.getKey(), i.getValue());
            }
            return t.request().get();
        }

        public Response post(String path, String content) {
            return this.post(path,
                    Entity.entity(content, MediaType.APPLICATION_JSON));
        }

        public Response post(String path, Entity<?> entity) {
            return this.target.path(path)
                    .request()
                    .post(entity);
        }

        public Response delete(String path, String id) {
            return this.target.path(path).path(id)
                    .request()
                    .delete();
        }
    }
}
