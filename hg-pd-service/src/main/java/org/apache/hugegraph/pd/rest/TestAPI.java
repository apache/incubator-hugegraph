package org.apache.hugegraph.pd.rest;

import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.StoreNodeEventType;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.PulseStore;
import org.apache.hugegraph.pd.pulse.ChangeType;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn.bond@hotmail.com on 2022/2/9
 */
@RestController
@Slf4j
@RequestMapping("/test")
public class TestAPI {

    @Autowired
    private PDConfig pdConfig;

    @GetMapping(value = "/discovery/{appName}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String discovery(@PathVariable(value = "appName", required = true) String appName) {
        RegistryService register = new RegistryService(pdConfig);
        // Query query=Query.newBuilder().setAppName("hugegraph").build();
        AtomicLong label = new AtomicLong();
        HashMap<String, String> labels = new HashMap<>();
        String labelValue = String.valueOf(label.incrementAndGet());
        //labels.put("address",labelValue);
        Query query = Query.newBuilder().build();
        // Query query = Query.newBuilder().setAppName("hugegraph").set.build();

        return register.getNodes(query).toString();
    }

    @GetMapping(value = "/pulse", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyClient() {
        PDPulseSubjects.notifyClient(getPartitionHeartbeatResponse());
        return "PartitionHeartbeatResponse";
    }

    @GetMapping(value = "/pulse/1", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyClientTo1() {
        return String.valueOf(PDPulseSubjects.notifyClient(getPartitionHeartbeatResponse(), 1L));
    }

    @GetMapping(value = "/pulse/12", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyClientTo12() {
        return String.valueOf(PDPulseSubjects.notifyClient(getPartitionHeartbeatResponse(), List.of(1L, 2L)));
    }

    @GetMapping(value = "/pulse/123", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyClientTo123() {
        return String.valueOf(PDPulseSubjects.notifyClient(getPartitionHeartbeatResponse(), List.of(1L, 2L, 3L)));
    }

    private PartitionHeartbeatResponse.Builder getPartitionHeartbeatResponse() {
        return PartitionHeartbeatResponse.newBuilder()
                .setPartition(Metapb.Partition.newBuilder()
                        .setId(8)
                        .setGraphName("graphName8"))
                .setChangeShard(
                        ChangeShard.newBuilder()
                                .setChangeTypeValue(8)
                                .addShard(Metapb.Shard.newBuilder()
                                        .setRoleValue(8)
                                        .setStoreId(8)
                                )
                );

    }

    @GetMapping(value = "/partition", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String noticePartition() {
        PDPulseSubjects.notifyPartitionChange(ChangeType.ALTER, "graph-test", 99);
        return "partition";
    }

    @GetMapping(value = "/shard-group", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String noticeShardGroup() {
        Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder().setId(88).build();
        PDPulseSubjects.notifyShardGroupChange(ChangeType.ALTER, 88, group);

        return group.toString();
    }

    @GetMapping(value = "/node", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyNodeChange() {
        PDPulseSubjects.notifyNodeChange(
                StoreNodeEventType.STORE_NODE_EVENT_TYPE_NODE_RAFT_CHANGE
                , "graph-test", 77
        );

        return "notifyNodeChange( STORE_NODE_EVENT_TYPE_NODE_RAFT_CHANGE, graph-test, 77)";
    }

    @GetMapping(value = "/graph", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyGraphChange() {
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("graph-meta").build();
        PDPulseSubjects.notifyGraphChange(graph);

        return graph.toString();
    }

    @PutMapping(value = "/queue", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String testPutQueue() {
        this.putQueue();
        return "queue";
    }

    public void putQueue() {
        PartitionHeartbeatResponse response = PartitionHeartbeatResponse.newBuilder()
                .setPartition(Metapb.Partition.newBuilder()
                        .setId(9)
                        .setGraphName("graphName"))
                .setChangeShard(
                        ChangeShard.newBuilder()
                                .setChangeTypeValue(9)
                                .addShard(Metapb.Shard.newBuilder()
                                        .setRoleValue(9)
                                        .setStoreId(9)
                                )
                ).build();

        Metapb.QueueItem.Builder builder = Metapb.QueueItem.newBuilder()
                .setItemId("item-id")
                .setItemClass("item-class")
                .setItemContent(response.toByteString());


        PulseStore store = MetadataFactory.newPulseStore(pdConfig);

        try {
            store.addItem(builder.setItemId("item-id-1").build());
            store.addItem(builder.setItemId("item-id-2").build());
            store.addItem(builder.setItemId("item-id-3").build());
        } catch (PDException e) {
            e.printStackTrace();
        }
        List<Metapb.QueueItem> queue = null;
        try {
            queue = store.getQueue();
        } catch (PDException e) {
            e.printStackTrace();
        }
        Parser<PartitionHeartbeatResponse> parser = PartitionHeartbeatResponse.parser();

        queue.stream().forEach(e -> {
            PartitionHeartbeatResponse buf = null;
            try {
                buf = parser.parseFrom(e.getItemContent());
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
            PDPulseSubjects.notifyClient(PartitionHeartbeatResponse.newBuilder(buf));
        });


    }
}
