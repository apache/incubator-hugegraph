package org.apache.hugegraph.pd.rest;

import com.baidu.hugegraph.pd.common.PDException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class API {

    public static String STATUS_KEY = "status";
    public static String ERROR_KEY = "error";
    public static String QUOTATION = "\"";
    public static String COMMA = ",";
    public static String COLON = ": ";
    public static final String VERSION = "3.6.3";
    public static final String PD = "PD";
    public static final String STORE = "STORE";


    public <T extends MessageOrBuilder> String toJSON(List<T> values, String key) {

        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0").append(COMMA)
                .append(QUOTATION).append(key).append(QUOTATION).append(COLON)
                .append("[ ");

        if (values != null) {
            values.forEach(s -> {
                try {
                    builder.append(JsonFormat.printer().print(s));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                builder.append(",");
            });
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("]}");
        return builder.toString();
    }

    public String toJSON(MessageOrBuilder value, String key) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0").append(COMMA)
                .append(QUOTATION).append(key).append(QUOTATION).append(COLON);
        try {
            if (value != null)
                builder.append(JsonFormat.printer().print(value));
            else
                builder.append("{}");
            builder.append("}");
            return builder.toString();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return toJSON(e);
        }

    }

    public <T extends MessageOrBuilder> String toJSON(Map<String, List<T>> values) {
        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        for (Map.Entry<String, List<T>> entry : values.entrySet()) {
            String entryKey = entry.getKey();
            List<T> entryValue = entry.getValue();
            builder.append(QUOTATION).append(entryKey).append(QUOTATION).append(COLON).append("[");
            if ((entryValue != null) && !(entryValue.isEmpty())) {
                entryValue.forEach(s -> {
                    try {
                        if (s == null){
                            builder.append("null");
                        }else{
                            builder.append(JsonFormat.printer().print(s));
                        }
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    builder.append(",");
                });
                builder.deleteCharAt(builder.length() - 1); //删除最后一个逗号
            }
            builder.append("]").append(COMMA);
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append("}");
        return builder.toString();
    }

    public String toJSON(PDException exception) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append(exception.getErrorCode()).append(COMMA)
                .append(QUOTATION).append(ERROR_KEY).append(QUOTATION).append(COLON)
                .append(QUOTATION).append(exception.getMessage()).append(QUOTATION);
        builder.append("}");

        return builder.toString();
    }

    public String toJSON(Exception exception) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("-1").append(COMMA)
                .append(QUOTATION).append(ERROR_KEY).append(QUOTATION).append(COLON)
                .append(QUOTATION).append(exception.getMessage()).append(QUOTATION);
        builder.append("}");

        return builder.toString();
    }

    /**
     * @param object
     * @return
     * @author tianxiaohui
     */
    public String toJSON(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    public Map<String, Object> okMap(String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put(STATUS_KEY, 0);
        map.put(k, v);
        return map;
    }

    public <T extends MessageOrBuilder> String toJSON(List<T> values, JsonFormat.TypeRegistry registry) {

        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append(QUOTATION).append(STATUS_KEY).append(QUOTATION).append(COLON).append("0").append(COMMA)
                .append(QUOTATION).append("log").append(QUOTATION).append(COLON)
                .append("[ ");
        JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(registry);
        if (values != null) {
            values.forEach(s -> {
                try {
                    builder.append(printer.print(s));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                builder.append(",");
            });
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("]}");
        return builder.toString();
    }

}
