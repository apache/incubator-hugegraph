package org.apache.hugegraph.pd.rest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.model.GraphRestRequest;
import org.apache.hugegraph.pd.model.GraphStatistics;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class GraphAPI extends API {
    @Autowired
    PDRestService pdRestService;
    @Autowired
    PDService pdService;

    @GetMapping(value = "/graph/partitionSizeRange", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getPartitionSizeRange() {
        try {
            int minPartitionSize = 1;
            int maxPartitionSize = pdService.getStoreNodeService().getShardGroups().size();
            Map<String, Integer> dataMap = new HashMap<>();
            dataMap.put("minPartitionSize", minPartitionSize);
            dataMap.put("maxPartitionSize", maxPartitionSize);
            return new RestApiResponse(dataMap, ErrorType.OK, ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException:", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    @GetMapping(value = "/graphs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraphs() {
        RestApiResponse response = new RestApiResponse();
        try {
            List<Metapb.Graph> graphs = pdRestService.getGraphs();
            List<GraphStatistics> resultGraphs = new ArrayList<>();
            for (Metapb.Graph graph : graphs) {
                if ((graph.getGraphName() != null) && (graph.getGraphName().endsWith("/g"))) {
                    resultGraphs.add(new GraphStatistics(graph, pdRestService, pdService));
                }
            }
            HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put("graphs", resultGraphs);
            response.setData(dataMap);
            response.setStatus(ErrorType.OK.getNumber());
            response.setMessage(ErrorType.OK.name());

        } catch (PDException e) {
            log.error("PDException: ", e);
            response.setData(new HashMap<String, Object>());
            response.setStatus(e.getErrorCode());
            response.setMessage(e.getMessage());
        }
        return response;
    }

    @PostMapping(value = "/graph/**", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setGraph(@RequestBody GraphRestRequest body, HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            final String prefix = "/graph/";
            final int limit = 2;
            String graphName = requestURL.split(prefix, limit)[1];
            graphName = URLDecoder.decode(graphName, "utf-8");
            Metapb.Graph curGraph = pdRestService.getGraph(graphName);
            Metapb.Graph.Builder builder =
                    Metapb.Graph.newBuilder(curGraph == null ? Metapb.Graph.getDefaultInstance() : curGraph);
            builder.setGraphName(graphName);
            if (body.getPartitionCount() > 0) {
                builder.setPartitionCount(body.getPartitionCount());
            }

            Metapb.Graph newGraph = pdRestService.updateGraph(builder.build());
            return toJSON(newGraph, "graph");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }


    @GetMapping(value = "/graph/**", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraph(HttpServletRequest request) throws UnsupportedEncodingException {
        RestApiResponse response = new RestApiResponse();
        GraphStatistics statistics;
        String requestURL = request.getRequestURL().toString();
        final String prefix = "/graph/";
        final int limit = 2;
        String graphName = requestURL.split(prefix, limit)[1];
        graphName = URLDecoder.decode(graphName, "utf-8");
        try {
            Metapb.Graph graph = pdRestService.getGraph(graphName);
            if (graph != null) {
                statistics = new GraphStatistics(graph, pdRestService, pdService);
                response.setData(statistics);
            } else {
                response.setData(new HashMap<String, Object>()); //没有该图
            }
            response.setStatus(ErrorType.OK.getNumber());
            response.setMessage(ErrorType.OK.name());
        } catch (PDException e) {
            log.error(e.getMessage());
            response.setData(new HashMap<String, Object>());
            response.setStatus(ErrorType.UNKNOWN.getNumber());
            response.setMessage(e.getMessage());
        }
        return response;
    }
}