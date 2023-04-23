package com.baidu.hugegraph.store.node.controller;

import com.alipay.sofa.jraft.core.NodeMetrics;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeService;
import com.baidu.hugegraph.store.node.metrics.DriveMetrics;
import com.baidu.hugegraph.store.node.metrics.SystemMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lynn.bond@hotmail.com on 2021/11/23
 */
@RestController
@RequestMapping(value = "/metrics",method = RequestMethod.GET)
public class HgStoreMetricsController {
    @Autowired
    HgStoreNodeService nodeService;

    private SystemMetrics systemMetrics = new SystemMetrics();
    private DriveMetrics driveMetrics = new DriveMetrics();

    @GetMapping
    public Map<String,String> index(){
        return new HashMap<>();
    }

    @GetMapping("system")
    public Map<String, Map<String, Object>> system(){
        return this.systemMetrics.metrics();
    }

    @GetMapping("drive")
    public Map<String,Map<String,Object>> drive(){
        return this.driveMetrics.metrics();
    }

    @GetMapping("raft")
    public Map<String, NodeMetrics> getRaftMetrics(){
        return nodeService.getNodeMetrics();
    }


}
