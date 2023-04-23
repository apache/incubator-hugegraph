package com.baidu.hugegraph.store.node.controller;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.PartitionEngine;
import com.baidu.hugegraph.store.business.BusinessHandler;
import com.baidu.hugegraph.store.metric.HgStoreMetric;
import com.baidu.hugegraph.store.business.InnerKeyCreator;
import com.baidu.hugegraph.store.node.AppConfig;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeService;
import com.baidu.hugegraph.store.meta.Partition;
import com.baidu.hugegraph.util.Bytes;
import com.taobao.arthas.agent.attach.ArthasAgent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@Slf4j
@RequestMapping("/v1")
public class PartitionAPI {
    @Autowired
    HgStoreNodeService nodeService;

    @Autowired
    AppConfig appConfig;

    @GetMapping(value = "/partitions", produces = "application/json")
    public Map<String, Object> getPartitions(@RequestParam(required= false, defaultValue = "") String flags) {

        boolean accurate = false;
        if(!flags.isEmpty()) {
            List<String> flagList = Arrays.asList(flags.split(","));
            if(flagList.contains("accurate")) {
                accurate = true;
            }
        }

        List<Raft> rafts = new ArrayList<>();
        HgStoreEngine storeEngine = nodeService.getStoreEngine();

        BusinessHandler businessHandler = storeEngine.getBusinessHandler();
        Map<Integer, PartitionEngine> partitionEngines = storeEngine.getPartitionEngines();

        for (Map.Entry<Integer, PartitionEngine> engineEntry : partitionEngines.entrySet()) {
            PartitionEngine engine = engineEntry.getValue();
            Raft raft = new Raft();
            raft.setGroupId(engine.getGroupId());
            raft.setLeader(engine.getLeader());
            raft.setRole(engine.getRaftNode().getNodeState().name());
            raft.setConf(engine.getRaftNode().getCurrentConf().toString());
            if (engine.isLeader()) {
                raft.setPeers(engine.getRaftNode().listPeers());
                raft.setLearners(engine.getRaftNode().listLearners());
            }
            raft.setTerm(engine.getLeaderTerm());
            raft.setLogIndex(engine.getCommittedIndex());
            raft.setPartitionCount(engine.getPartitions().size());
            for(Map.Entry<String, Partition> partitionEntry : engine.getPartitions().entrySet()) {
                String graphName = partitionEntry.getKey();
                Partition pt = partitionEntry.getValue();
                PartitionInfo partition = new PartitionInfo(pt);
                // 此处为了打开所有的图，metric只返回已打开的图
                businessHandler.getLatestSequenceNumber(graphName, pt.getId());
                partition.setMetric(businessHandler.getPartitionMetric(graphName, pt.getId(), accurate));
                partition.setLeader(pt.isLeader() == engine.isLeader() ? "OK" : "Error");
                raft.getPartitions().add(partition);
            }
            rafts.add(raft);
        }

        return okMap("partitions", rafts);
    }

    @GetMapping(value = "/partition/{id}", produces = "application/json")
    public Raft getPartition(@PathVariable(value = "id") int id) {

        HgStoreEngine storeEngine = nodeService.getStoreEngine();

        BusinessHandler businessHandler = storeEngine.getBusinessHandler();
        PartitionEngine engine = storeEngine.getPartitionEngine(id);

        Raft raft = new Raft();
        raft.setGroupId(engine.getGroupId());
        raft.setLeader(engine.getLeader());
        raft.setRole(engine.getRaftNode().getNodeState().name());
        if (engine.isLeader()) {
            raft.setPeers(engine.getRaftNode().listPeers());
            raft.setLearners(engine.getRaftNode().listLearners());
        }
        raft.setLogIndex(engine.getCommittedIndex());
        for (Map.Entry<String, Partition> partitionEntry : engine.getPartitions().entrySet()) {
            String graphName = partitionEntry.getKey();
            Partition pt = partitionEntry.getValue();
            PartitionInfo partition = new PartitionInfo(pt);
            partition.setMetric(businessHandler.getPartitionMetric(graphName, pt.getId(), false));

            raft.getPartitions().add(partition);
        }

        return raft;
        //return okMap("partition", rafts);
    }
    /**
     * 打印分区的所有key
     */
    @GetMapping(value = "/partition/dump/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> dumpPartition(@PathVariable(value = "id") int id) throws PDException {
        HgStoreEngine storeEngine = nodeService.getStoreEngine();
        BusinessHandler handler = storeEngine.getBusinessHandler();
        InnerKeyCreator innerKeyCreator = new InnerKeyCreator(handler);
        storeEngine.getPartitionEngine(id).getPartitions().forEach((graph, partition) -> {
            log.info("{}----------------------------", graph);
            ScanIterator cfIterator = handler.scanRaw(graph, partition.getId(), 0);
            while (cfIterator.hasNext()) {
                try (ScanIterator iterator = cfIterator.next()) {
                    byte[] cfName = cfIterator.position();
                    log.info("\t{}", new String(cfName));
                    while (iterator.hasNext()) {
                        RocksDBSession.BackendColumn col = iterator.next();
                        int keyCode = innerKeyCreator.parseKeyCode(col.name);
                        log.info("\t\t{} --key={}, code={} ", new String(col.name),
                                 Bytes.toHex(col.name), keyCode);
                    }
                }
            }
            cfIterator.close();
        });
        return okMap("ok", null);
    }

    /**
     * 打印分区的所有key
     */
    @GetMapping(value = "/partition/clean/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> cleanPartition(@PathVariable(value = "id") int id) throws PDException {
        HgStoreEngine storeEngine = nodeService.getStoreEngine();
        BusinessHandler handler = storeEngine.getBusinessHandler();

        storeEngine.getPartitionEngine(id).getPartitions().forEach((graph, partition) -> {
           handler.cleanPartition(graph, id);
        });
        return okMap("ok", null);
    }

    @GetMapping(value = "/arthasstart", produces = "application/json")
    public Map<String, Object> arthasstart(@RequestParam(required= false, defaultValue = "") String flags) {
        HashMap<String, String> configMap = new HashMap<String, String>();
        configMap.put("arthas.telnetPort", appConfig.getArthasConfig().getTelnetPort());
        configMap.put("arthas.httpPort", appConfig.getArthasConfig().getHttpPort());
        configMap.put("arthas.ip", appConfig.getArthasConfig().getArthasip());
        configMap.put("arthas.disabledCommands", appConfig.getArthasConfig().getDisCmd());
        ArthasAgent.attach(configMap);
//        DashResponse retPose = new DashResponse();
        List<String> ret = new ArrayList<>();
        ret.add("Arthas 启动成功");
        return okMap("arthasstart", ret);
    }
    @Data
    public class Raft{
        private int groupId;
        private String role;
        private String conf;
        private Endpoint leader;
        private long term;
        private long logIndex;
        private List<PeerId> peers;
        private List<PeerId> learners;
        private int partitionCount;
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


        public PartitionInfo(com.baidu.hugegraph.store.meta.Partition pt) {
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

