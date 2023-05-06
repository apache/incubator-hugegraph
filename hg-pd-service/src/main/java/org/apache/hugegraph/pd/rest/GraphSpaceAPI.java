package org.apache.hugegraph.pd.rest;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;

import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.model.GraphSpaceRestRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.List;

@RestController
@Slf4j
@RequestMapping("/v1")
public class GraphSpaceAPI extends API{
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

    @PostMapping(value = "/graph-spaces/**",  consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setGraphSpace(@RequestBody GraphSpaceRestRequest body, HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            String graphSpaceName = requestURL.split("/graph-spaces/", 2)[1];
            graphSpaceName = URLDecoder.decode(graphSpaceName, "utf-8");
            Metapb.GraphSpace graphSpace = Metapb.GraphSpace.newBuilder()
                    .setName(graphSpaceName)
                    .setStorageLimit(body.getStorageLimit())
                    .build();
            Metapb.GraphSpace newGraphSpace =  pdRestService.setGraphSpace(graphSpace);
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
            graphSpaceName = URLDecoder.decode(graphSpaceName, "utf-8");
            Metapb.GraphSpace graphSpace = pdRestService.getGraphSpace(graphSpaceName);
            return toJSON(graphSpace, "graphs-paces");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }


}
