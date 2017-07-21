package com.baidu.hugegraph.api;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EdgeApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/graph/edges/";

    @BeforeClass
    public static void setup() {
        // add some edges (NOTE: vertices have been added before)
        String look2 = "{"
                + "\"label\":\"look\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-2\","
                + "\"properties\":{"
                + "\"time\":\"2017-5-18\""
                + "}}";
        String look3 = "{"
                + "\"label\":\"look\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-3\","
                + "\"properties\":{"
                + "\"time\":\"2017-5-18\""
                + "}}";

        Response r = newClient().post(path, look2);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());

        r = newClient().post(path, look3);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());
    }

    @AfterClass
    public static void teardown() {
        newClient().delete(path, "author:1>look>2017-5-18>book:java-3");
    }

    @Test
    public void testCreate() {
        String edge = "{"
                + "\"label\":\"authored\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-1\","
                + "\"properties\":{"
                + "\"contribution\":\"2017-5-18\""
                + "}}";
        System.out.println(client().post(path, edge).readEntity(String.class));
        Assert.assertEquals(201, client().post(path, edge).getStatus());
    }

    @Test
    public void testGet() {
        String edge = "author:1>look>2017-5-18>book:java-2";
        Response r = client().get(path, edge);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGetNotFound() {
        String edge = "author:1>look>2017-5-18>book:!not-exists!";
        Response r = client().get(path, edge);
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
        String edge = "author:1>look>2017-5-18>book:java-3";
        Response r = client().delete(path, edge);
        Assert.assertEquals(204, r.getStatus());
    }
}
