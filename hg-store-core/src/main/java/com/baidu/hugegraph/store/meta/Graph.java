package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;

import lombok.Data;

@Data
public class Graph implements Cloneable {
    private String graphName;
    private Metapb.Graph graph;

    public Graph() {

    }

    public Graph(Metapb.Graph protoObj) {
        graphName = protoObj.getGraphName();
        this.graph = protoObj;
    }

    public Metapb.Graph getProtoObj() {
        // return Metapb.Graph.newBuilder()
        //        .setGraphName(graphName)
        //        .build();
        return this.graph;
    }

    public void setProtoObj(Metapb.Graph protoObj) {
        // return Metapb.Graph.newBuilder()
        //        .setGraphName(graphName)
        //        .build();
        this.graph = protoObj;
    }

    public Graph clone() {
        try {
            return (Graph) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

}
