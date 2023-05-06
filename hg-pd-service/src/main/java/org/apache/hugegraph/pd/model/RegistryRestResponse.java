package org.apache.hugegraph.pd.model;

import com.baidu.hugegraph.pd.grpc.Pdpb;
import lombok.Data;

import java.io.Serializable;

/**
 * @author zhangyingjie
 * @date 2022/2/8
 **/
@Data
public class RegistryRestResponse {

    Pdpb.ErrorType errorType;
    String message;
    Serializable data;

}
