package org.apache.hugegraph.pd.model;

import lombok.Data;

import java.util.HashMap;

/**
 * @author zhangyingjie
 * @date 2022/2/8
 **/
@Data
public class RegistryQueryRestRequest {

    String appName;
    String version;
    HashMap<String,String> labels;
}
