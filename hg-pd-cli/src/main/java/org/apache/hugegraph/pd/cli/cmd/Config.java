package org.apache.hugegraph.pd.cli.cmd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;

/**
 * @author zhangyingjie
 * @date 2023/10/17
 **/
public class Config extends Command {

    public Config(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) throws PDException {
        String param = params[0];
        String[] pair = param.split("=");
        String key = pair[0].trim();
        Object value = null;
        if (pair.length > 1) {
            value = pair[1].trim();
        }
        if (value == null) {
            Metapb.PDConfig pdConfig = pdClient.getPDConfig();
            switch (key) {
                case "enableBatchLoad":
                    //    value = pdConfig.getEnableBatchLoad();
                    break;
                case "shardCount":
                    value = pdConfig.getShardCount();
                    break;
            }

            System.out.println("Get config " + key + "=" + value);
        } else {
            Metapb.PDConfig.Builder builder = Metapb.PDConfig.newBuilder();
            switch (key) {
                case "enableBatchLoad":
                    //   builder.setEnableBatchLoad(Boolean.valueOf((String)value));
                case "shardCount":
                    builder.setShardCount(Integer.valueOf((String) value));
            }
            pdClient.setPDConfig(builder.build());
            System.out.println("Set config " + key + "=" + value);
        }
    }
}
