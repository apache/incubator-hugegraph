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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.model.GraphSpaceRestRequest;
import org.apache.hugegraph.pd.service.PDRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class GraphSpaceAPI extends API {

    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/graph-spaces", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getGraphSpaces() {
        try {
            List<Metapb.GraphSpace> graphSpaces = pdRestService.getGraphSpaces();
            return toJSON(graphSpaces, "graph-spaces");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @PostMapping(value = "/graph-spaces/**", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setGraphSpace(@RequestBody GraphSpaceRestRequest body,
                                HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            String graphSpaceName = requestURL.split("/graph-spaces/", 2)[1];
            graphSpaceName = URLDecoder.decode(graphSpaceName, StandardCharsets.UTF_8);
            Metapb.GraphSpace graphSpace = Metapb.GraphSpace.newBuilder()
                                                            .setName(graphSpaceName)
                                                            .setStorageLimit(body.getStorageLimit())
                                                            .build();
            Metapb.GraphSpace newGraphSpace = pdRestService.setGraphSpace(graphSpace);
            return toJSON(newGraphSpace, "graph-spaces");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }

    @GetMapping(value = "/graph-spaces/**", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getGraphSpace(HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            String graphSpaceName = requestURL.split("/graph-spaces/", 2)[1];
            graphSpaceName = URLDecoder.decode(graphSpaceName, StandardCharsets.UTF_8);
            Metapb.GraphSpace graphSpace = pdRestService.getGraphSpace(graphSpaceName);
            return toJSON(graphSpace, "graphs-paces");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }

}
