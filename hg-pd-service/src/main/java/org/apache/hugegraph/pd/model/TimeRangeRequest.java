package org.apache.hugegraph.pd.model;

import lombok.Data;

/**
 * @author zhangyingjie
 * @date 2022/3/23
 **/
@Data
public class TimeRangeRequest {
    String startTime;
    String endTime;
}
