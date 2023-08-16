package org.apache.hugegraph.metrics;

public enum MetricsKeys {
    // 最大响应时间 Maximum response time
    // 平均响应时间 Mean response time
    // 请求总数 Total Requests
    // 失败数   Failed Requests
    // 成功数   Success Requests

    MAX_RESPONSE_TIME(1, "MAX_RESPONSE_TIME"),

    MEAN_RESPONSE_TIME(2, "MEAN_RESPONSE_TIME"),

    TOTAL_REQUEST(3, "TOTAL_REQUEST"),

    FAILED_REQUEST(4, "FAILED_REQUEST"),

    SUCCESS_REQUEST(5, "SUCCESS_REQUEST"),
    ;

    private final byte code;
    private final String name;


    MetricsKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }
}
