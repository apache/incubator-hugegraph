package org.apache.hugegraph.pd.model;

import lombok.Data;

import java.util.HashMap;

/**
 * @author zhangyingjie
 * @date 2022/2/8
 **/
@Data
public class RegistryRestRequest {

    String id;
    String appName;
    String version;
    String address;
    String interval;
    HashMap<String,String> labels;
}
