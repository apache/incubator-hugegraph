package com.baidu.hugegraph.store.cmd;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DestroyRaftRequest extends HgCmdBase.BaseRequest{
    private List<String> graphNames = new ArrayList<>();

    public void addGraphName(String graphName) {
        graphNames.add(graphName);
    }
    @Override
    public byte magic() {
        return HgCmdBase.DESTROY_RAFT;
    }
}
