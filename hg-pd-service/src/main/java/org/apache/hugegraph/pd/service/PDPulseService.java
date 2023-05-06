package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import com.baidu.hugegraph.pd.grpc.pulse.PulseRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PulseResponse;
import com.baidu.hugegraph.pd.meta.MetadataFactory;
import com.baidu.hugegraph.pd.meta.QueueStore;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import com.baidu.hugegraph.pd.raft.RaftEngine;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */

@Slf4j
@GRpcService
public class PDPulseService extends HgPdPulseGrpc.HgPdPulseImplBase {

    @Autowired
    private PDConfig pdConfig;

    private QueueStore queueStore=null;

    public PDPulseService(){
        PDPulseSubject.setQueueRetrieveFunction(()->getQueue());
        PDPulseSubject.setQueueDurableFunction(getQueueDurableFunction());
        PDPulseSubject.setQueueRemoveFunction(getQueueRemoveFunction());
    }

    @Override
    public StreamObserver<PulseRequest> pulse(StreamObserver<PulseResponse> responseObserver) {
        return PDPulseSubject.addObserver(responseObserver);
    }

    private static Supplier<List<Metapb.QueueItem>> queueRetrieveFunction = () -> Collections.emptyList();
    private static Function<Metapb.QueueItem, Boolean> queueDurableFunction = (e) -> true;
    private static Function<String, Boolean> queueRemoveFunction = (e) -> true;


    private Function<String, Boolean> getQueueRemoveFunction(){
        return itemId->{
            try{
                this.getQueueStore().removeItem(itemId);
                return true;
            }catch (Throwable t){
                log.error("Failed to remove item from store, item-id: "+itemId+", cause by:",t);
            }
            return false;
        };
    }

    private Function<Metapb.QueueItem, Boolean> getQueueDurableFunction(){
        return item->{
            try{
                this.getQueueStore().addItem(item);
                return true;
            }catch (Throwable t){
                log.error("Failed to add item to store, item: "+item.toString()+", cause by:",t);
            }
            return false;
        };
    }

    private boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    private List<Metapb.QueueItem> getQueue(){

        if(!isLeader()){
            return Collections.emptyList();
        }

        try{
            return this.getQueueStore().getQueue();
        }catch (Throwable t){
            log.error("Failed to retrieve queue from QueueStore, cause by:",t);
        }

        log.warn("Returned empty queue list.");
        return Collections.emptyList();
    }

    private QueueStore getQueueStore(){
        if(this.queueStore==null){
            this.queueStore=MetadataFactory.newQueueStore(pdConfig);
        }
        return this.queueStore;
    }
}
