package org.apache.hugegraph.pd.cli.cmd;

import lombok.Data;

/**
 * @author zhangyingjie
 * @date 2023/10/20
 **/
@Data
public class Parameter {
    String cmd;
    String pd;
    String[] params;
    String separator;
}
