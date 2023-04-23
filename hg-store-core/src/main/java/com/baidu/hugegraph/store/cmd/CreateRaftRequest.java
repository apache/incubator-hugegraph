package com.baidu.hugegraph.store.cmd;

import com.alipay.sofa.jraft.conf.Configuration;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CreateRaftRequest extends HgCmdBase.BaseRequest{
    public List<Metapb.Partition> getPartitions() {
        try {
            List<Metapb.Partition> partitions = new ArrayList<>();
            for (byte[] partition : values)
                partitions.add(Metapb.Partition.parseFrom(partition));
            return partitions;
        } catch (InvalidProtocolBufferException e) {
            log.error("CreateRaftNodeProcessor parse partition exception }", e);
        }
        return new ArrayList<>();
    }
    public void addPartition(Metapb.Partition partition) {
        values.add(partition.toByteArray());
    }

    public void setConf(Configuration conf) {
        if ( conf != null ){
            this.peers = conf.toString();
        }
    }

    public Configuration getConf() {
        Configuration conf = null;
        if ( peers != null) {
            conf = new Configuration();
            conf.parse(this.peers);
        }
        return conf;
    }
    List<byte[]> values = new ArrayList<>();
    String peers;
    @Override
    public byte magic() {
        return HgCmdBase.CREATE_RAFT;
    }
}
