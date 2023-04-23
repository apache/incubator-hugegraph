package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.util.Asserts;
import com.baidu.hugegraph.store.util.Version;
import lombok.Data;

import java.util.Map;

/**
 * @projectName: hugegraph-store
 * @package: com.baidu.hugegraph.store.core
 * @className: HgStoreNode
 * @author: tyzer
 * @description: TODO
 * @date: 2021/10/19 19:44
 * @version: 1.0
 */
@Data
public class Store {
    private long id = 0;
    private String storeAddress;
    private String pdAddress;
    private String raftAddress;
    private String version;
    private String deployPath;
    private String dataPath; // 数据存储路径
    private int dataVersion;
    private int partitionCount;
    private int startTime;
    private int usedSize;     //rocksdb存储大小
    private int pdHeartbeatInterval;
    private Metapb.StoreState state;
    private Map<String, String> labels;
    private int cores;

    public Store(){
        this.id = 0;
        this.version = Version.getVersion();
    }
    public Store(int dataVersion){
        this.id = 0;
        this.dataVersion = dataVersion;
        this.version = Version.getVersion();
    }
    public Store(Metapb.Store protoObj){
        if ( protoObj != null) {
            this.id = protoObj.getId();
            this.raftAddress = protoObj.getRaftAddress();
            this.storeAddress = protoObj.getAddress();
            this.dataVersion = protoObj.getDataVersion();
        }
        else
            this.id = 0;
        this.version = Version.getVersion();
    }


    public Metapb.Store getProtoObj(){
        Asserts.isNonNull(storeAddress);
        Asserts.isNonNull(raftAddress);
        Metapb.Store.Builder builder = Metapb.Store.newBuilder()
                .setId(id).setVersion(version)
                .setDataVersion(dataVersion)
                .setAddress(storeAddress)
                .setRaftAddress(raftAddress)
                .setState(Metapb.StoreState.Up)
                .setCores(cores)
                .setDeployPath(deployPath)
                .setDataPath(dataPath);

        if (labels != null)
            labels.forEach((k,v)->{
                builder.addLabels(Metapb.StoreLabel.newBuilder().setKey(k).setValue(v).build());
            });

        return builder.build();

    }
    public boolean checkState(Metapb.StoreState state) {
        return this.state == state;
    }
}
