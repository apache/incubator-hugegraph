package org.apache.hugegraph.pd.rest;

import com.baidu.hugegraph.pd.RegistryService;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.discovery.Query;
import com.baidu.hugegraph.pd.grpc.pulse.ChangeShard;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import com.baidu.hugegraph.pd.meta.MetadataFactory;
import com.baidu.hugegraph.pd.meta.QueueStore;

import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import org.apache.hugegraph.pd.watch.PDWatchSubject;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

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
    public String discovery(@PathVariable(value = "appName", required = true)String appName){
        RegistryService register =new RegistryService(pdConfig);
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
        PDPulseSubject.notifyClient(
                PartitionHeartbeatResponse.newBuilder()
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
                        )

        );
        return "partition";
    }

    @GetMapping(value = "/partition", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String noticePartition() {
        PDWatchSubject.notifyPartitionChange(PDWatchSubject.ChangeType.ALTER, "graph-test", 99);
        return "partition";
    }

    @PutMapping(value = "/queue", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String testPutQueue() {
        this.putQueue();
        return "queue";
    }

    public void putQueue(){
        PartitionHeartbeatResponse response=PartitionHeartbeatResponse.newBuilder()
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

        Metapb.QueueItem.Builder builder=Metapb.QueueItem.newBuilder()
                .setItemId("item-id")
                .setItemClass("item-class")
                .setItemContent(response.toByteString());


        QueueStore store= MetadataFactory.newQueueStore(pdConfig);

        try {
            store.addItem(builder.setItemId("item-id-1").build());
            store.addItem(builder.setItemId("item-id-2").build());
            store.addItem(builder.setItemId("item-id-3").build());
        } catch (PDException e) {
            e.printStackTrace();
        }
        List<Metapb.QueueItem> queue=null;
        try {
            queue=store.getQueue();
        } catch (PDException e) {
            e.printStackTrace();
        }
        Parser<PartitionHeartbeatResponse> parser= PartitionHeartbeatResponse.parser();

        queue.stream().forEach(e->{
            PartitionHeartbeatResponse buf=null;
            try {
                buf=parser.parseFrom(e.getItemContent());
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
            PDPulseSubject.notifyClient( PartitionHeartbeatResponse.newBuilder(buf));
        });



    }
}
