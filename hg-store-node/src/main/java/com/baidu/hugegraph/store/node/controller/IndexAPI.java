package com.baidu.hugegraph.store.node.controller;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.metric.HgStoreMetric;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeService;
import com.baidu.hugegraph.store.meta.Partition;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/")
public class IndexAPI {
    @Autowired
    HgStoreNodeService nodeService;

    @GetMapping(value = "/", produces = "application/json")
    public StoreInfo index() {
        StoreInfo info = new StoreInfo();
        info.leaderCount = nodeService.getStoreEngine().getLeaderPartition().size();
        info.partitionCount = nodeService.getStoreEngine().getPartitionEngines().size();
        return info;
    }

    @Data
    class StoreInfo{
        private int leaderCount;
        private int partitionCount;
    }
    @Data
    public class Raft{
        private int groupId;
        private String role;
        private String conf;
        private Endpoint leader;
        private long logIndex;
        private List<PeerId> peers;
        private List<PeerId> learners;
        private List<PartitionInfo> partitions = new ArrayList<>();
    }

    @Data
    public class PartitionInfo {
        private int id;                                             // region id
        private String graphName;
        // Region key range [startKey, endKey)
        private long startKey;
        private long endKey;
        private HgStoreMetric.Partition metric;
        private String version;
        private Metapb.PartitionState workState;

        private String leader;


        public PartitionInfo(Partition pt) {
            id = pt.getId();
            graphName = pt.getGraphName();
            startKey = pt.getStartKey();
            endKey = pt.getEndKey();

            workState = pt.getWorkState();
            version = String.valueOf(pt.getVersion());

        }
    }

    public Map<String, Object> okMap(String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put("status", 0);
        map.put(k, v);
        return map;
    }
}

