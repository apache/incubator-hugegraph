package org.apache.hugegraph.pd.util;

import static org.apache.hugegraph.pd.grpc.common.ErrorType.CLIENT_INVALID_PARAMETER_VALUE;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.OK_VALUE;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.PD_NOT_LEADER_VALUE;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.PD_UNAVAILABLE_VALUE;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.WARNING_VALUE;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import com.google.protobuf.util.JsonFormat;

/**
 * @author zhangyingjie
 * @date 2024/1/11
 **/
public class ResponseUtil {

    private static Map<Integer, String> details = new HashMap<>() {{
        put(OK_VALUE, "OK");
        put(WARNING_VALUE, "The correct result is obtained, but switching nodes is a better option");
        put(PD_UNAVAILABLE_VALUE, "The Pd cannot provide services. Switch the node and try again");
        put(PD_NOT_LEADER_VALUE, "The Pd is not the leader. Switch the node and try again");
        put(CLIENT_INVALID_PARAMETER_VALUE, "The parameters passed by the client are incorrect");
    }};

    public static String getMessageByCode(int code) {
        ErrorType type = ErrorType.forNumber(code);
        if (type != null) {
            return type.name();
        } else {
            return "";
        }
    }

    public static String getDetailByCode(int code) {
        String detail = details.get(code);
        if (detail != null) {
            return detail;
        } else {
            return getMessageByCode(code);
        }
    }

    public static String toJson(ResponseHeader header) {
        try {
            return JsonFormat.printer().print(header);
        } catch (Exception e) {
            return "{}";
        }
    }
}
