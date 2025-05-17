<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
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

package org.apache.hugegraph.pd.rest;
========
package org.apache.hugegraph.pd.service;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
========
/**
 * @author tianxiaohui
 * @date 20221220
 **/
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
public class RestApiTest extends BaseServerTest {

    @Test
    public void testQueryClusterInfo() throws URISyntaxException, IOException, InterruptedException,
                                              JSONException {
        String url = pdRestAddr + "/v1/cluster";
        HttpRequest request = HttpRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .uri(new URI(url))
========
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
    public void testQueryClusterMembers() throws URISyntaxException, IOException,
                                                 InterruptedException, JSONException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
========
    public void testQueryClusterMembers() throws URISyntaxException, IOException, InterruptedException,
                                                 JSONException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryStoresInfo() throws URISyntaxException, IOException, InterruptedException,
                                             JSONException {
        String url = pdRestAddr + "/v1/stores";
        HttpRequest request = HttpRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .uri(new URI(url))
========
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryGraphsInfo() throws IOException, InterruptedException, JSONException,
                                             URISyntaxException {
        String url = pdRestAddr + "/v1/graphs";
        HttpRequest request = HttpRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .uri(new URI(url))
========
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryPartitionsInfo() throws IOException, InterruptedException, JSONException,
                                                 URISyntaxException {
        String url = pdRestAddr + "/v1/highLevelPartitions";
        HttpRequest request = HttpRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .uri(new URI(url))
========
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryDebugPartitionsInfo() throws URISyntaxException, IOException,
                                                      InterruptedException {
        String url = pdRestAddr + "/v1/partitions";
        HttpRequest request = HttpRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .uri(new URI(url))
========
                                         .uri(new URI(url)).header(key, value)
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200;
    }

    @Test
    public void testQueryShards() throws URISyntaxException, IOException, InterruptedException,
                                         JSONException {
        String url = pdRestAddr + "/v1/shards";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/rest/RestApiTest.java
                                         .GET()
                                         .build();
========
                                         .header(key, value)
                                         .GET()
                                         .build();

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/RestApiTest.java
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }
}
