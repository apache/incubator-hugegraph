package org.apache.hugegraph.pd.model;

import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import lombok.Data;

import java.io.Serializable;

/**
 * @author zhangyingjie
 * @date 2022/2/8
 **/
@Data
public class RegistryRestResponse {

    ErrorType errorType;
    String message;
    Serializable data;

}
