package org.apache.hugegraph.pd.model;

import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author tianxiaohui
 * @date 2022-07-21
 */
@Data
public class RestApiResponse {
    String message;
    Object data;
    int status;

    public RestApiResponse(Object data, ErrorType status, String message) {
        if (data == null){
            data = new HashMap<String, Object>();
        }
        this.data = data;
        this.status = status.getNumber();
        this.message = message;
    }

    public RestApiResponse() {

    }

    public RestApiResponse(Object data, int status, String message){
        if (data == null){
            data = new HashMap<String, Object>();
        }
        this.data = data;
        this.status = status;
        this.message = message;
    }
}
