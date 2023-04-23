package com.baidu.hugegraph.store.raft.util;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.List;

public class RaftUtils {
    public static  List<String> getAllEndpoints(Node node){
        List<String> endPoints = new ArrayList<>();
        node.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        node.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }
    public static  List<String> getAllEndpoints(Configuration conf){
        List<String> endPoints = new ArrayList<>();
        conf.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        conf.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }
    public static List<String> getPeerEndpoints(Node node){
        List<String> endPoints = new ArrayList<>();
        node.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getPeerEndpoints(Configuration conf){
        List<String> endPoints = new ArrayList<>();
        conf.listPeers().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }

    public static List<String> getLearnerEndpoints(Node node){
        List<String> endPoints = new ArrayList<>();
        node.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }
    public static List<String> getLearnerEndpoints(Configuration conf){
        List<String> endPoints = new ArrayList<>();
        conf.listLearners().forEach(peerId -> {
            endPoints.add(peerId.getEndpoint().toString());
        });
        return endPoints;
    }
    public static boolean configurationEquals(Configuration oldConf, Configuration newConf){
        return ListUtils.isEqualList(oldConf.listPeers(), newConf.listPeers()) &&
                ListUtils.isEqualList(oldConf.listLearners(), newConf.listLearners());
    }
}
