package com.baidu.hugegraph.api;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class VertexApiTest extends BaseTest {

    private static String url = "/graphs/hugegraph/graph/vertices/";

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
        Assert.assertEquals(201, newClient().post(url, lisa).getStatus());
        Assert.assertEquals(201, newClient().post(url, hebe).getStatus());
    }

    @AfterClass
    public static void teardown() {
        newClient().delete(url, "person%02Lisa");
        newClient().delete(url, "person%02Hebe");
    }

    @Test
    public void testCreate() {
        String vertex = "{\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}}";
        Assert.assertEquals(201, client().post(url, vertex).getStatus());
    }

    @Test
    public void testGet() {
        String vertex = "person%02Lisa";
        Response r = client().get(url, vertex);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGetNotFound() {
        String vertex = "person%02!not-exists-this-vertex!";
        Response r = client().get(url, vertex);
        // TODO: improve to 404 (currently server returns 400 if not found)
        Assert.assertEquals(400, r.getStatus());
    }

    @Test
    public void testList() {
        Response r = client().get(url);
        System.out.println("testList(): " + r.readEntity(String.class));
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testDelete() {
        String vertex = "person%02Lisa";
        Response r = client().delete(url, vertex);
        Assert.assertEquals(204, r.getStatus());
    }
}
