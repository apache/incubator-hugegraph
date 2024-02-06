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

package org.apache.hugegraph.backend.store.palo;

import java.util.Map;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rest.AbstractRestClient;
import org.apache.hugegraph.rest.RestClient;
import org.apache.hugegraph.rest.RestHeaders;

import com.google.common.collect.ImmutableMap;

import okhttp3.Response;

public class PaloHttpClient {

    private final RestClient client;

    public PaloHttpClient(HugeConfig config, String database) {
        String url = this.buildUrl(config, database);
        String username = config.get(PaloOptions.PALO_USERNAME);
        String password = config.get(PaloOptions.PALO_PASSWORD);
        Integer timeout = config.get(PaloOptions.PALO_HTTP_TIMEOUT);

        this.client = new Client(url, username, password, timeout);
    }

    private String buildUrl(HugeConfig config, String database) {
        String host = config.get(PaloOptions.PALO_HOST);
        Integer port = config.get(PaloOptions.PALO_HTTP_PORT);
        return String.format("http://%s:%s/api/%s/", host, port, database);
    }

    public void bulkLoadAsync(String table, String body, String label) {
        // Format path
        String path = table + "/_load";
        // Format headers
        RestHeaders headers = new RestHeaders();
        headers.add("Expect", "100-continue");
        // Format params
        Map<String, Object> params = ImmutableMap.of("label", label);
        // Send request
        this.client.put(path, body, headers, params);
    }

    private static class Client extends AbstractRestClient {

        private static final int SECOND = 1000;

        public Client(String url, String user, String password, int timeout) {
            super(url, user, password, timeout * SECOND);
        }

        @Override
        protected void checkStatus(Response response, int... statuses) {
            // pass
        }
    }
}
