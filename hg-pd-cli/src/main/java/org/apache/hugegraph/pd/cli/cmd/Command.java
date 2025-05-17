package org.apache.hugegraph.pd.cli.cmd;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;

/**
 * @author zhangyingjie
 * @date 2023/10/17
 **/
public abstract class Command {

    protected static String error = "启动参数: 命令, pd地址, 命令参数, 参数分隔符(非必须)";
    protected PDClient pdClient;
    protected PDConfig config;

    public Command(String pd) {
        config = PDConfig.of(pd).setAuthority("store", "");
        pdClient = PDClient.create(config);
    }

    public static Parameter toParameter(String[] args) throws PDException {
        if (args.length < 2) {
            throw new PDException(-1, error);
        }
        Parameter parameter = new Parameter();
        parameter.setCmd(args[0]);
        parameter.setPd(args[1]);

        if (args.length == 2) {
            parameter.setParams(new String[0]);
            return parameter;
        }

       if (args.length == 4) {
           // 之前的逻辑，存在一个分隔符，做兼容
           String t = args[3];
           if (t != null && !t.isEmpty() && args[2].contains(t)) {
               parameter.setParams(args[2].split(t));
               parameter.setSeparator(t);
               return parameter;
           }
       }

       // 剩余的部分放到 params中
       String[] params = new String[args.length - 2] ;
       System.arraycopy(args, 2, params, 0, args.length - 2);
       parameter.setParams(params);

        return parameter;
    }

    public abstract void action(String[] params) throws Exception;
}
