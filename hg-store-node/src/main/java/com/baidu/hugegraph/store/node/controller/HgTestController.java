package com.baidu.hugegraph.store.node.controller;

import com.baidu.hugegraph.store.PartitionEngine;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeService;
import com.baidu.hugegraph.store.meta.Partition;
import com.baidu.hugegraph.store.meta.Store;
import com.baidu.hugegraph.store.cmd.HgCmdProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * 仅用于测试
 */
@RestController
@Slf4j
@RequestMapping("/test")
public class HgTestController {
    @Autowired
    HgStoreNodeService nodeService;

    @GetMapping(value = "/leaderStore", produces = MediaType.APPLICATION_JSON_VALUE)
    public Store testGetStoreInfo() {

        Store store = null;
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(0);

        for(Partition partition : engine.getPartitions().values()){
            store = nodeService.getStoreEngine().getHgCmdClient()
                    .getStoreInfo(engine.getLeader().toString());
        };
        return store;
    }

    @GetMapping(value = "/raftRestart/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String restartRaftNode(@PathVariable(value = "groupId") int groupId) {
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(groupId);
        engine.restartRaftNode();
        return "OK";
    }

    @GetMapping(value = "/raftDelete/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String deleteRaftNode(@PathVariable(value = "groupId") int groupId) {
        List<String> graphs = new ArrayList<>();
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(groupId);
        if ( engine != null ){
            engine.getPartitions().forEach((k, v)->{
                graphs.add(v.getGraphName());
            });
            nodeService.getStoreEngine().destroyPartitionEngine(groupId, graphs);
            return "OK";
        }else
            return "未找到分区";


    }

    @GetMapping(value = "/gc", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doGc() {
        System.gc();
        return "gc OK!";
    }

    @GetMapping(value = "/flush", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doFlush() {
        nodeService.getStoreEngine().getBusinessHandler().flushAll();
        return "flush all!";
    }

    @GetMapping(value = "/close", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doCloseAll() {
        nodeService.getStoreEngine().getBusinessHandler().closeAll();
        return "close all!";
    }
    @GetMapping(value = "/snapshot", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doSnapshot() {
        nodeService.getStoreEngine().getPartitionEngines().forEach((k,v)->{
            v.snapshot();
        });
        return "snapshot OK!";
    }

    @GetMapping(value = "/compact", produces = MediaType.APPLICATION_JSON_VALUE)
    public String dbCompaction() {
        nodeService.getStoreEngine().getPartitionEngines().forEach((k,v)->{
            nodeService.getStoreEngine().getBusinessHandler().dbCompaction("", k);
        });
        return "snapshot OK!";
    }
}
