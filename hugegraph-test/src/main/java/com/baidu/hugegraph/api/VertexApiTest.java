package com.baidu.hugegraph.api;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class VertexApiTest extends BaseTest {

    private static String path = "/graphs/hugegraph/graph/vertices/";

    @BeforeClass
    public static void setup() {
        // add some vertices
        String lisa = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"Lisa\","
                + "\"city\":\"Beijing\","
                + "\"age\":20"
                + "}}";
        String hebe = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"Hebe\","
                + "\"city\":\"Taipei\","
                + "\"age\":21"
                + "}}";

        Response r = newClient().post(path, lisa);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());

        r = newClient().post(path, hebe);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());
    }

    @AfterClass
    public static void teardown() {
        newClient().delete(path, "person%02Lisa");
        newClient().delete(path, "person%02Hebe");
    }

    @Test
    public void testCreate() {
        String vertex = "{\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}}";
        Assert.assertEquals(201, client().post(path, vertex).getStatus());
    }

    @Test
    public void testGet() {
        String vertex = "person%02Lisa";
        Response r = client().get(path, vertex);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGetNotFound() {
        String vertex = "person%02!not-exists-this-vertex!";
        Response r = client().get(path, vertex);
        // TODO: improve to 404 (currently server returns 400 if not found)
        Assert.assertEquals(400, r.getStatus());
    }

    @Test
    public void testList() {
        Response r = client().get(path);
        System.out.println("testList(): " + r.readEntity(String.class));
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testDelete() {
        String vertex = "person%02Lisa";
        Response r = client().delete(path, vertex);
        Assert.assertEquals(204, r.getStatus());
    }
}
