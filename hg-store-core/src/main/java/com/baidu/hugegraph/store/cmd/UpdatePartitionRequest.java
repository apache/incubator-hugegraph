package com.baidu.hugegraph.store.cmd;

import com.baidu.hugegraph.pd.grpc.Metapb;
import lombok.Data;


@Data
public class UpdatePartitionRequest extends HgCmdBase.BaseRequest{
    private int startKey;
    private int endKey;

    private Metapb.PartitionState workState;
    @Override
    public byte magic() {
        return HgCmdBase.RAFT_UPDATE_PARTITION;
    }
}
