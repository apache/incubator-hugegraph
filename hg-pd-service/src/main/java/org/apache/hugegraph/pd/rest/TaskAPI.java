package org.apache.hugegraph.pd.rest;

import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;

import org.apache.hugegraph.pd.service.PDRestService;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/v1/task")
public class TaskAPI extends API {
    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/patrolStores", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolStores() {
        try {
            List<Metapb.Store> stores = pdRestService.patrolStores();
            return toJSON(stores, "stores");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/patrolPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolPartitions() {
        try {
            List<Metapb.Partition> partitions = pdRestService.patrolPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/balancePartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<Integer, KVPair<Long, Long>>  balancePartitions() {
        try {
            Map<Integer, KVPair<Long, Long>> partitions = pdRestService.balancePartitions();
            return partitions;
        } catch (PDException e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping(value = "/splitPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String  splitPartitions() {
        try {
            List<Metapb.Partition> partitions  = pdRestService.splitPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }
    @GetMapping(value = "/balanceLeaders")
    public Map<Integer, Long> balanceLeaders() throws PDException {
        return pdRestService.balancePartitionLeader();
    }

    @GetMapping(value = "/compact")
    public String dbCompaction() throws PDException {
        pdRestService.dbCompaction();
        return "compact ok";
    }
}
